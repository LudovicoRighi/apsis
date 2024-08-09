import asyncio
from   collections import deque
import logging
import math
from   mmap import PAGESIZE
from   ora import now, Time
import resource
import sys
import traceback

from   .actions import Action
from   .cond.base import PolledCondition, RunStoreCondition, NonmonotonicRunStoreCondition
from   .host_group import config_host_groups
from   .jobs import Jobs, load_jobs_dir, diff_jobs_dirs
from   .lib.asyn import cancel_task, TaskGroup
from   .lib.sys import to_signal
from   .program.base import _InternalProgram
from   .program.base import Output, OutputMetadata
from   .program.base import ProgramRunning, ProgramError, ProgramFailure, ProgramSuccess, ProgramUpdate
from   .program.procstar.agent import start_server
from   . import runs
from   .run_log import RunLog
from   .run_snapshot import snapshot_run
from   .runs import Run, RunStore, RunError, MissingArgumentError, ExtraArgumentError
from   .runs import get_bind_args
from   .scheduled import ScheduledRuns
from   .scheduler import Scheduler, get_runs_to_schedule
from   .states import State
from   .service.client import Client

log = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

class Apsis:
    """
    The gestalt scheduling application.

    Responsible for:

    - Assembling subcomponents:
      - job repo
      - persistent database
      - scheduler
      - scheduled

    - Managing run transitions:
      - handing runs from one component to the next
      - applying and persisting transitions

    - Exposing a high-level API for user run operations

    """

    def __init__(self, cfg, jobs, db):
        log.debug("creating Apsis instance")
        self.__start_time = now()

        # Start a loop to monitor the async event loop.
        self.__check_async_task = asyncio.ensure_future(self.__check_async())
        self.__check_async_stats = {}

        self.cfg = cfg
        # FIXME: This should go in `apsis.config.config_globals` or similar.
        config_host_groups(cfg)
        self.__db = db

        self.run_log = RunLog(self.__db.run_log_db)
        self.jobs = Jobs(jobs, db.job_db)

        # Actions applied to all runs.
        self.__actions = [ Action.from_jso(o) for o in cfg["actions"] ]

        try:
            lookback = cfg["runs"]["lookback"]
        except KeyError:
            min_timestamp = None
        else:
            min_timestamp = now() - lookback
        self.run_store = RunStore(db, min_timestamp=min_timestamp)

        procstar_cfg = cfg.get("procstar", {}).get("agent", {})
        if procstar_cfg.get("enable", False):
            log.info("starting procstar server")
            server_coro = start_server(procstar_cfg)
            self.__procstar_task = asyncio.ensure_future(server_coro)
        else:
            self.__procstar_task = None

        log.info("scheduling runs")
        self.scheduled = ScheduledRuns(db.clock_db, self.__wait)
        # For now, expose the output database directly.
        self.outputs = db.output_db
        # Tasks for waiting runs.
        self.__wait_tasks = TaskGroup(log)
        # Tasks for starting/running runs.
        self.__run_tasks = TaskGroup(log)
        # Tasks for running actions.
        self.__action_tasks = TaskGroup(log)

        # Continue scheduling from the last time we handled scheduled jobs.
        # FIXME: Rename: schedule horizon?
        stop_time = db.clock_db.get_time()
        self.scheduler = Scheduler(cfg, self.jobs, self.schedule, stop_time)

        self.__retire_loop = asyncio.ensure_future(self.retire_loop())


    async def restore(self):
        """
        Restores scheduled, waiting, and running runs from DB.
        """
        try:
            log.info("restoring")

            # Restore scheduled runs from DB.
            log.info("restoring scheduled runs")
            _, scheduled_runs = self.run_store.query(state=State.scheduled)
            for run in scheduled_runs:
                assert not run.expected
                if not self.__prepare_run(run):
                    continue
                time = run.times["schedule"]
                self.run_log.record(run, "restored")
                await self.scheduled.schedule(time, run)

            # Restore waiting runs from DB.
            log.info("restoring waiting runs")
            _, waiting_runs = self.run_store.query(state=State.waiting)
            for run in waiting_runs:
                assert not run.expected
                if not self.__prepare_run(run):
                    continue
                self.run_log.record(run, "restored")
                self.__wait(run)

            # If a run is starting in the DB, we can't know if it actually
            # started or not, so mark it as error.
            log.info("processing starting runs")
            _, starting_runs = self.run_store.query(state=State.starting)
            for run in starting_runs:
                self.run_log.record(run, "restored starting: might have started")
                self._transition(
                    run, run.STATE.error,
                    message="restored starting: might have started"
                )

            # Reconnect to running runs.
            _, running_runs = self.run_store.query(state=State.running)
            log.info("reconnecting running runs")
            for run in running_runs:
                self.__reconnect(run)

            log.info("restoring done")

        except Exception:
            log.critical("restore failed", exc_info=True)
            raise SystemExit(1)


    def start_loops(self):
        # Set up the scheduler.
        log.info("starting scheduler loop")
        self.__scheduler_task = asyncio.ensure_future(self.scheduler.loop())

        # Set up the manager for scheduled tasks.
        log.info("starting scheduled loop")
        self.__scheduled_task = asyncio.ensure_future(self.scheduled.loop())


    def __wait(self, run):
        """
        Starts waiting for `run`.
        """
        if len(run.conds) == 0:
            # No conditions to wait for.  Start immediately.
            self.__start(run)
            return

        # Make sure the run is waiting.
        if run.state != run.STATE.waiting:
            self.run_log.record(run, "waiting")
            self._transition(run, run.STATE.waiting)

        # If a max waiting time is configured, compute the timeout for this run.
        cfg = self.cfg.get("waiting", {})
        timeout = cfg.get("max_time", None)

        def wait_with_timeout(cond_wait):
            """
            Timeout wrapper.

            :param cond_wait:
              The cond's wait coro.
            """
            if timeout is None:
                waiting_timeout = None
            else:
                # Adjust timeout to be relative to the time the run started waiting.
                try:
                    waiting_time = run.times["waiting"]
                except KeyError:
                    log.error(f"no waiting time in run: {run}")
                    waiting_timeout = timeout
                else:
                    # Subtract time we have already waited from the timeout.
                    waiting_timeout = timeout - (now() - waiting_time)

            return asyncio.wait_for(cond_wait, waiting_timeout)


        async def loop():
            """
            The wait loop for a single run.
            """
            conds = list(run.conds)

            if len(conds) > 0:
                self.run_log.record(run, f"waiting until: {conds[0]}")

            while len(conds) > 0:
                cond = conds[0]
                try:
                    match cond:
                        case PolledCondition():
                            result = await wait_with_timeout(cond.wait())

                        # Special handling for nonmonotonic conditions involving
                        # other runs.  After async waiting the condition, check
                        # one more time synchronously before continuing
                        # (synchronously) to transition.
                        case NonmonotonicRunStoreCondition():
                            while True:
                                result = await wait_with_timeout(cond.wait(self.run_store))
                                # Last check, synchronously.
                                result = cond.check(self.run_store)
                                if result is not False:
                                    break

                        case RunStoreCondition():
                            result = await wait_with_timeout(cond.wait(self.run_store))

                        case _:
                            assert False, f"not implemented: {cond!r}"

                except asyncio.CancelledError:
                    raise

                except asyncio.TimeoutError:
                    msg = f"waiting for {cond}: timeout after {timeout} s"
                    self.run_log.info(run, msg)
                    self._transition(run, Run.STATE.error)
                    return

                except Exception:
                    msg = f"waiting for {cond}: failed"
                    log.error(msg, exc_info=True)
                    self._run_exc(run, message=msg)
                    return

                match result:
                    case True:
                        # The condition is satisfied.  Proceed to the next.
                        conds.pop(0)
                        if len(conds) > 0:
                            self.run_log.record(run, f"waiting until: {conds[0]}")

                    case cond.Transition(state):
                        # Force a transition.
                        self.run_log.info(
                            run, result.reason or f"{state}: {cond}")
                        self._transition(run, state)
                        # Don't wait for further conds.
                        return

                    case _:
                        assert False, f"invalid condition wait result: {result!r}"

            else:
                # Start the run.
                self.__start(run)

        # Start the waiting task.
        self.__wait_tasks.add(run.run_id, loop())


    def __start(self, run):
        """
        Starts a run.

        Runs the run's program in a task added to `__run_tasks`.
        """
        # Start the run by running its program.
        self.run_log.record(run, "starting")
        self._transition(run, State.starting)
        # Call the program.  This produces an async iterator of updates.
        updates = run.program.run(
            run.run_id,
            self if isinstance(run.program, _InternalProgram) else self.cfg,
        )
        self.__run_tasks.add(run.run_id, self.__do_program(run, aiter(updates)))


    def __reconnect(self, run):
        """
        Reconnects to a running run.

        Finishes running the run's program in a task added to `__run_tasks`.
        """
        assert run.state == State.running
        assert run.run_state is not None
        self.run_log.record(run, "reconnecting")
        # Connect to the program.  This produces an async iterator of updates.
        updates = run.program.connect(
            run.run_id,
            run.run_state,
            self if isinstance(run.program, _InternalProgram) else self.cfg,
        )
        self.__run_tasks.add(run.run_id, self.__do_program(run, aiter(updates)))


    async def __do_program(self, run, updates):
        """
        Processes `updates` for `run` until the run is finished.
        """
        try:
            if run.state == State.starting:
                update = await anext(updates)
                log.debug(f"run update: {run.run_id}: {update}")
                match update:
                    case ProgramRunning() as running:
                        self.run_log.record(run, "running")
                        self._transition(
                            run, State.running,
                            run_state   =running.run_state,
                            meta        =running.meta,
                            times       =running.times,
                        )

                    case ProgramError() as error:
                        self.run_log.info(run, f"error: {error.message}")
                        self._transition(
                            run, State.error,
                            meta        =error.meta,
                            times       =error.times,
                            outputs     =error.outputs,
                        )
                        return

                    case _ as update:
                        assert False, f"unexpected update: {update}"

            assert run.state == State.running

            while run.state == State.running:
                update = await anext(updates)
                log.debug(f"run update: {run.run_id}: {update}")
                match update:
                    case ProgramUpdate() as update:
                        self._update(
                            run,
                            outputs     =update.outputs,
                            meta        =update.meta,
                        )

                    case ProgramSuccess() as success:
                        self.run_log.record(run, "success")
                        self._transition(
                            run, State.success,
                            meta        =success.meta,
                            times       =success.times,
                            outputs     =success.outputs,
                        )

                    case ProgramFailure() as failure:
                        # Program ran and failed.
                        self.run_log.record(run, f"failure: {failure.message}")
                        self._transition(
                            run, State.failure,
                            meta        =failure.meta,
                            times       =failure.times,
                            outputs     =failure.outputs,
                        )

                    case ProgramError() as error:
                        self.run_log.info(run, f"error: {error.message}")
                        self._transition(
                            run, State.error,
                            meta        =error.meta,
                            times       =error.times,
                            outputs     =error.outputs,
                        )

                    case _ as update:
                        assert False, f"unexpected update: {update}"

            else:
                # Exhaust the async iterator, so that cleanup can run.
                try:
                    update = await anext(updates)
                except StopAsyncIteration:
                    # Expected.
                    pass
                else:
                    assert False, f"unexpected update: {update}"

        except (asyncio.CancelledError, StopAsyncIteration):
            log.info(f"run task cancelled: {run.run_id}")
            # We do not transition the run here.  The run can survive an Apsis
            # restart and we can connect to it later.

        except Exception:
            # Program raised some other exception.
            self.run_log.exc(run, "error: internal")
            tb = traceback.format_exc().encode()
            output = Output(OutputMetadata("traceback", length=len(tb)), tb)
            self._transition(
                run, State.error,
                outputs ={"output": output}
            )


    def __prepare_run(self, run):
        """
        Prepares a run for schedule or restore, using its job.

        The run must already be in the run DB.

        On failure, transitions the run to error and returns false.
        """
        try:
            job = self._validate_run(run)
        except Exception as exc:
            self._run_exc(run, message=str(exc))
            return False

        if run.program is None:
            try:
                run.program = job.program.bind(get_bind_args(run))
            except Exception as exc:
                self._run_exc(run, message=f"invalid program: {exc}")
                return False

        if run.conds is None:
            try:
                run.conds = [ c.bind(run, self.jobs) for c in job.conds ]
            except Exception as exc:
                self._run_exc(run, message=f"invalid condition: {exc}")
                return False

        # Attach job labels to the run.
        if run.meta.get("labels") is None:
            run.meta["labels"] = job.meta.get("labels", [])

        return True


    def __start_actions(self, run):
        """
        Starts configured actions on `run` as tasks.
        """
        # Find actions attached to the job.
        # FIXME: Would be better to bind actions to the run and serialize them
        # along with the run, as we do with programs.
        job_id = run.inst.job_id
        try:
            job = self.jobs.get_job(job_id)
        except LookupError:
            # Job is gone; can't get the actions.
            self.run_log.exc(run, "no job")
            job = None
            actions = []
        else:
            actions = list(job.actions)

        # Include actions configured globally for all runs.
        actions += self.__actions

        # Check conditions on actions.
        actions = [
            a for a in actions
            if a.condition is None or a.condition(run)
        ]

        if len(actions) == 0:
            # Nothing to do.
            return

        async def wrap(run, snapshot, action):
            """
            Wrapper to handle exceptions from an action.

            Since these are run as background tasks, we don't transition the
            run to error if an action fails.
            """
            try:
                await action(self, snapshot)
            except Exception:
                self.run_log.exc(run, "action")

        # The actions run in tasks and the run may transition again soon, so
        # hand the actions a snapshot instead.
        snapshot = snapshot_run(self, run)

        for action in actions:
            key = (id(action), run.run_id, run.state)
            self.__action_tasks.add(key, wrap(run, snapshot, action))


    # --- Internal API ---------------------------------------------------------

    def _run_exc(self, run, *, message=None):
        """
        Transitions `run` to error, with the current exception attached.
        """
        self.run_log.exc(run, message)

        outputs = {}
        exc_type, exc, _ = sys.exc_info()
        if exc_type is not None:
            # Attach the exception traceback as run output.
            if issubclass(exc_type, RuntimeError):
                msg = str(exc).encode()
            else:
                msg = traceback.format_exc().encode()
            # FIXME: For now, use the name "output" as this is the only one
            # the UIs render.  In the future, change to "traceback".
            outputs["output"] = Output(
                OutputMetadata("output", len(msg), content_type="text/plain"),
                msg
            )

        self._transition(
            run, run.STATE.error,
            times={"error": now()},
            message=message,
            outputs=outputs,
        )


    def _update(self, run, *, outputs=None, meta=None):
        """
        Updates run state without transitioning.
        """
        assert run.state in {State.starting, State.running}

        if meta is not None:
            run._update(meta=meta)
        if outputs is not None:
            # FIXME: We absolutely need to do better than this to avoid crazy
            # rewriting to the DB.
            for output_id, output in outputs.items():
                self.__db.output_db.upsert(run.run_id, output_id, output)

        self.run_store.update(run, now())


    def _transition(self, run, state, *, outputs={}, **kw_args):
        """
        Transitions `run` to `state`, updating it with `kw_args`.
        """
        time = now()

        # A run is no longer expected once it is no longer scheduled.
        if run.expected and state not in {State.new, State.scheduled}:
            self.__db.run_log_db.flush(run.run_id)
            run.expected = False

        # Transition the run object.
        run._transition(time, state, **kw_args)

        # Persist outputs.
        # FIXME: We are persisting outputs assuming all are new.  This is only
        # OK for the time being because outputs are always added on the final
        # transition.  In general, we have to persist new outputs only.
        for output_id, output in outputs.items():
            self.__db.output_db.upsert(run.run_id, output_id, output)

        # Persist the new state.
        self.run_store.update(run, time)

        self.__start_actions(run)


    def _validate_run(self, run):
        """
        :raise RuntimeError:
          `run` is invalid.
        """
        # The job ID must be valid.
        job = self.jobs.get_job(run.inst.job_id)

        # Args must match job params.
        args = frozenset(run.inst.args)
        missing, extra = job.params - args, args - job.params
        if missing:
            raise MissingArgumentError(run, *missing)
        if extra:
            raise ExtraArgumentError(run, *extra)

        return job


    def _propagate_args(self, old_args, inst):
        job = self.jobs.get_job(inst.job_id)
        args = runs.propagate_args(old_args, job, inst.args)
        return runs.Instance(inst.job_id, args)


    # --- API ------------------------------------------------------------------

    # FIXME: Move the API elsewhere.

    async def schedule(self, time, run):
        """
        Adds and schedules a new run.

        This is the only way to add a new run to the scheduler instance.

        :param time:
          The schedule time at which to run the run.  If `None`, the run
          is run now, instead of scheduled.
        :return:
          The run, either scheduled or error.
        """
        time = None if time is None else Time(time)

        self.run_store.add(run)
        if not self.__prepare_run(run):
            return run

        if time is None:
            self.run_log.record(run, "scheduled: now")
            self._transition(run, run.STATE.scheduled, times={"schedule": now()})
            self.__wait(run)
            return run
        else:
            self.run_log.record(run, f"scheduled: {time}")
            self._transition(run, run.STATE.scheduled, times={"schedule": time})
            await self.scheduled.schedule(time, run)
            return run


    async def skip(self, run):
        """
        Skips a scheduled or waiting run.

        Unschedules the run and sets it to the skipped state.
        """
        if run.state == run.STATE.scheduled:
            self.scheduled.unschedule(run)

        elif run.state == run.STATE.waiting:
            # Cancel the waiting task.
            await self.__wait_tasks.cancel(run.run_id)

        else:
            raise RunError(f"can't skip {run.run_id} in state {run.state.name}")

        self.run_log.info(run, "skipped")
        self._transition(run, run.STATE.skipped, message="skipped")


    async def start(self, run):
        """
        Immediately starts a scheduled or waiting run.
        """
        if run.state == run.STATE.scheduled:
            self.scheduled.unschedule(run)
            self.run_log.info(run, "scheduled run started by override")
            self.__wait(run)

        elif run.state == run.STATE.waiting:
            # Cancel the waiting task.  It will remove itself when done.
            await self.__wait_tasks.cancel(run.run_id)
            self.run_log.info(run, "waiting run started by override")
            # Start the run.
            self.__start(run)

        else:
            raise RunError(f"can't start {run.run_id} in state {run.state.name}")


    async def mark(self, run, state):
        """
        Transitions a run to a different finished `state`.

        :param run:
          Run, in a finished state.
        :param state:
          A different finished state.
        """
        if state not in Run.FINISHED:
            raise RunError(f"can't mark {run.run_id} to state {state.name}")
        elif run.state not in Run.FINISHED:
            raise RunError(f"can't mark {run.run_id} in state {run.state.name}")
        elif state == run.state:
            raise RunError(f"run {run.run_id} already in state {state.name}")
        else:
            self._transition(run, state, force=True)
            self.run_log.info(run, f"marked as {state.name}")


    async def get_run_log(self, run_id):
        """
        Returns the run log for a run.
        """
        # Make sure the run ID is valid.
        self.run_store.get(run_id)
        return self.__db.run_log_db.query(run_id=run_id)


    async def rerun(self, run, *, time=None):
        """
        Creates a rerun of `run`.

        :param time:
          The time at which to schedule the rerun.  If `None`, runs the rerun
          immediately.
        """
        # Create the new run.
        log.info(f"rerun: {run.run_id} at {time or 'now'}")
        timestamp = now()
        new_run = Run(run.inst)
        await self.schedule(time, new_run)
        self.run_log.info(
            new_run, f"scheduled as rerun of {run.run_id}", timestamp=timestamp)
        return new_run



    async def riprovall(self, run, *, time=None):
        """
        Creates a riprovall of `run`.

        :param time:
          The time at which to schedule the riprovall.  If `None`, runs the riprovall
          immediately.
        """
        log.info(f"riprovall starting from run: {run.run_id} at {time or 'now'}")
        # mark all downstreams dependencies as failed
        log.info("++++++++++++++++++++++++++++++++++++++++++++++++++++")
        # for jj in self.jobs.get_jobs():
        #     log.info("######### CHECKING THE JOB WITH ID:")
        #     log.info(jj.job_id)
        #     log.info("######### THIS IS THE LIST OF CONDITIONS:")
        #     for cc in jj.conds:
        #         log.info(cc)

        #     log.info("######### THIS IS THE LIST OF DIRECT DOWNSTREAMS:")
        #     for dd in jj.downs:
        #         log.info(dd)

        #     log.info("------------------------------------------------")

        # log.info("++++++++++++++++++++++++++++++++++++++++++++++++++++")

        # root_job_id = run.inst.job_id

        # queue = deque([root_job_id])
        # to_rerun = set()

        # while queue:
        #     q_size = len(queue)
        #     for i in range(q_size):
        #         cur_id = queue.pop()
        #         cur_job = self.jobs.get_job(cur_id)
        #         if cur_id in to_rerun:
        #             continue
        #         queue.extend(list(cur_job.downs))        
        #         to_rerun.add(cur_id)

        # client = Client()
        # log.info("000000000000000000000000000")
        # log.info(to_rerun)        
        # log.info(client)
        # log.info("111111111111111111111111")

        # # INVALIDATE ALL THE RUNS THAT HAS TO BE.
        # for i, job_id in enumerate(to_rerun):
        #     log.info(f"TO RERUN {i}: {job_id}")
        #     job_runs = client.get_job_runs(job_id)
        #     log.info("RRRRRRRRRRRRR")
        #     log.info(len(job_runs))
            

        #     # for to_invalidate in client.get_job_runs(job_id):
        #     #     log.info(to_invalidate)
        #     #     log.info(type(to_invalidate))
        #     #     # await self.mark(to_invalidate, State.failure)

        # # Create the new runall.
        # log.info(f"riprovall (i.e. with downstreams): {run.run_id} at {time or 'now'}")
        # timestamp = now()
        # new_run = Run(run.inst)
        # await self.schedule(time, new_run)
        # self.run_log.info(
        #     new_run, f"scheduled as riprovall of {run.run_id}", timestamp=timestamp)
        # return new_run


    async def send_signal(self, run, signal):
        """
        :raise RuntimeError:
          `run` is not running.
        """
        signal = to_signal(signal)
        if run.state != State.running:
            raise RuntimeError(f"invalid run state for signal: {run.state.name}")
        assert run.program is not None

        self.run_log.info(run, f"sending {signal.name}")
        try:
            await run.program.signal(run.run_id, run.run_state, signal)
        except Exception:
            self.run_log.exc(run, f"sending {signal.name} failed")
            raise RuntimeError(f"sending {signal.name} failed")


    async def shut_down(self):
        log.info("shutting down Apsis")
        await cancel_task(self.__scheduler_task, "scheduler", log)
        await cancel_task(self.__scheduled_task, "scheduled", log)
        await self.__action_tasks.cancel_all()
        await self.__wait_tasks.cancel_all()
        await self.__run_tasks.cancel_all()
        await self.run_store.shut_down()
        await cancel_task(self.__check_async_task, "check async", log)
        if self.__procstar_task is not None:
            await cancel_task(self.__procstar_task, "procstar server", log)
        log.info("Apsis shut down")


    async def __check_async(self):
        """
        Monitors the async event loop.
        """
        while True:
            # Wake up on the next round 10 seconds.
            t = now()
            next_second = t.EPOCH + math.ceil((t - t.EPOCH + 0.01) / 10) * 10
            await asyncio.sleep(next_second - t)

            # See how late we are.
            latency = now() - next_second
            self.__check_async_stats = {
                "latency": latency,
            }


    def get_stats(self):
        res = resource.getrusage(resource.RUSAGE_SELF)
        stats = {
            "start_time"            : str(self.__start_time),
            "time"                  : str(now()),
            "async"                 : self.__check_async_stats,
            "rusage_maxrss"         : res.ru_maxrss * 1024,
            "rusage_utime"          : res.ru_utime,
            "rusage_stime"          : res.ru_stime,
            "tasks": {
                "num_waiting"       : len(self.__wait_tasks),
                "num_running"       : len(self.__run_tasks),
                "num_action"        : len(self.__action_tasks),
            },
            "len_runlogdb_cache"    : len(self.__db.run_log_db._RunLogDB__cache),
            "scheduled"             : self.scheduled.get_stats(),
            "run_store"             : self.run_store.get_stats(),
        }

        try:
            with open("/proc/self/statm") as file:
                statm = list(map(int, next(file).strip().split()))
        except FileNotFoundError:
            pass
        else:
            stats.update({
                "statm_size"        : statm[0] * PAGESIZE,
                "statm_resident"    : statm[1] * PAGESIZE,
            })

        return stats


    async def retire_loop(self):
        """
        Periodically retires runs older than `runs.lookback`.
        """
        log.info("starting retire loop")
        runs_cfg = self.cfg.get("runs", {})
        retire_lookback = runs_cfg.get("lookback", None)
        if retire_lookback is None:
            log.info("no runs.lookback in config; no retire loop")
            return

        while True:
            try:
                min_timestamp = now() - retire_lookback
                self.run_store.retire_old(min_timestamp)
            except Exception:
                log.error("retire failed", exc_info=True)
                return

            await asyncio.sleep(60)



#-------------------------------------------------------------------------------

def _unschedule_runs(apsis, job_id):
    """
    Deletes all scheduled expected runs of `job_id`.
    """
    _, runs = apsis.run_store.query(job_id=job_id, state=Run.STATE.scheduled)
    runs = [ r for r in runs if r.expected ]

    for run in runs:
        log.info(f"removing: {run.run_id}")
        apsis.scheduled.unschedule(run)
        apsis.run_store.remove(run.run_id)


async def reschedule_runs(apsis, job_id):
    """
    Reschedules runs of `job_id`.

    Unschedules and deletes existing scheduled runs.  Thenq rebuilds and
    reschedules runs according to the current job schedules.
    """
    scheduler = apsis.scheduler
    scheduled = apsis.scheduled

    # Get the time up to which scheduled runs were started.
    scheduled_time = scheduled.get_scheduled_time()
    # Get the time up to which jobs were scheduled.
    scheduler_time = scheduler.get_scheduler_time()

    # Unschedule all runs of this job.
    _unschedule_runs(apsis, job_id)

    # Restore scheduled runs, by rebuilding them between the scheduled time
    # and the scheduler time.
    job = apsis.jobs.get_job(job_id)
    schedule = list(get_runs_to_schedule(job, scheduled_time, scheduler_time))
    for time, run in schedule:
        await apsis.schedule(time, run)


async def reload_jobs(apsis, *, dry_run=False):
    """
    :param dry_run:
      If true, determine changes but don't apply them.
    :return:
      Job IDs that have been removed, job IDs that have been added, and job IDs
      that have changed.
    """
    # FIXME: Refactor to avoid using private attributes and methods.
    jobs0       = apsis.jobs._Jobs__jobs_dir
    job_db      = apsis.jobs._Jobs__job_db

    # Reload the contents of the jobs dir.
    log.info(f"reloading jobs from {jobs0.path}")
    jobs1 = load_jobs_dir(jobs0.path)

    # Diff them.
    rem_ids, add_ids, chg_ids = diff_jobs_dirs(jobs0, jobs1)

    if not dry_run:
        for job_id in rem_ids:
            log.info(f"unscheduling removed job: {job_id}")
            _unschedule_runs(apsis, job_id)
        for job_id in chg_ids:
            log.info(f"unscheduling changed job: {job_id}")
            _unschedule_runs(apsis, job_id)

        # Use the new jobs, including for scheduling.
        apsis.jobs = Jobs(jobs1, job_db)
        apsis.scheduler.set_jobs(apsis.jobs)

        for job_id in add_ids:
            log.info(f"scheduling added job: {job_id}")
            await reschedule_runs(apsis, job_id)
        for job_id in sorted(chg_ids):
            log.info(f"scheduling changed: {job_id}")
            await reschedule_runs(apsis, job_id)

    return rem_ids, add_ids, chg_ids


