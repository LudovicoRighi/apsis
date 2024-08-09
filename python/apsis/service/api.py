import asyncio
import logging
import ora
import re
import sanic
import ujson
from   urllib.parse import unquote
import websockets

from   apsis.apsis import reschedule_runs
from   apsis.lib.api import response_json, error, time_to_jso, to_bool, encode_response
import apsis.lib.itr
from   apsis.lib.timing import Timer
from   apsis.lib.sys import to_signal
from   ..jobs import jso_to_job
from   ..runs import Instance, Run, RunError

log = logging.getLogger(__name__)

# Max number of runs to send in one websocket message.
WS_RUN_CHUNK = 4096
WS_RUN_CHUNK_SLEEP = 0.001

#-------------------------------------------------------------------------------

API = sanic.Blueprint("v1")

@API.exception(RunError)
def no_such_process_error(request, exception):
    return error(exception, status=400)


@API.route("/alive")
async def live(request):
    return response_json({})


@API.route("/stats")
async def stats(request):
    stats = request.app.apsis.get_stats()
    return response_json(stats)


#-------------------------------------------------------------------------------

def to_state(state):
    return None if state is None else Run.STATE[state]


def _to_jso(obj):
    return None if obj is None else {
        **obj.to_jso(),
        "str": str(obj),
    }


def _to_jsos(objs):
    return [] if objs is None else [ _to_jso(o) for o in objs ]


def _job_to_jso(app, job):
    return {
        "job_id"        : job.job_id,
        "params"        : list(sorted(job.params)),
        "schedules"     : [ _to_jso(s) for s in job.schedules ],
        "program"       : _to_jso(job.program),
        "condition"     : [ _to_jso(c) for c in job.conds ],
        "actions"       : [ _to_jso(a) for a in job.actions ],
        "metadata"      : job.meta,
        "ad_hoc"        : job.ad_hoc,
        "url"           : app.url_for("v1.job", job_id=job.job_id),
    }


# FIXME: Clean up, or put back caching.
# No caching; jobs may change.
job_to_jso = _job_to_jso


def _run_summary_to_jso(app, run):
    jso = run._jso_cache
    if jso is not None:
        # Use the cached JSO.
        return jso

    # Construct the set of valid operations for this run.
    operations = set()
    # Start now or skip a scheduled or waiting job.
    if run.state in {run.STATE.scheduled, run.STATE.waiting}:
        operations.add("start")
        operations.add("skip")
    # Retry is available if the run didn't succeed.
    if run.state in run.FINISHED:
        operations.add("rerun")
    # Terminate and kill are available for a running run.
    if run.state == run.STATE.running:
        operations.add("terminate")
        operations.add("kill")
    # Mark actions are available among finished states
    for state in Run.FINISHED:
        if run.state != state and run.state in Run.FINISHED:
            operations.add(f"mark {state.name}")

    jso = {
        "job_id"        : run.inst.job_id,
        "args"          : run.inst.args,
        "run_id"        : run.run_id,
        "state"         : run.state.name,
        "times"         : { n: time_to_jso(t) for n, t in run.times.items() },
        "labels"        : run.meta.get("labels", []),
        "operations"    : sorted(operations),
    }
    if run.expected:
        jso["expected"] = run.expected
    if run.message is not None:
        jso["message"] = run.message

    run._jso_cache = jso
    return jso


def run_to_jso(app, run, summary=False):
    if run.state is None:
        # This run is being deleted.
        # FIXME: Hack.
        return {"run_id": run.run_id, "state": None}

    jso = _run_summary_to_jso(app, run)

    if not summary:
        jso = {
            **jso,
            "conds": _to_jsos(run.conds),
            # FIXME: Rename to metadata.
            "meta": run.meta,
            "program": _to_jso(run.program),
        }

    return jso


def runs_to_jso(app, when, runs, summary=False):
    return {
        "when": time_to_jso(when),
        "runs": { r.run_id: run_to_jso(app, r, summary) for r in runs },
    }


def _output_metadata_to_jso(app, run_id, outputs):
    return [
        {
            "output_id": output_id,
            "output_len": output.length,
        }
        for output_id, output in outputs.items()
    ]


#-------------------------------------------------------------------------------
# Jobs

class JobLookupError(LookupError):
    pass


@API.exception(JobLookupError)
def job_lookup_error(request, exception):
    return error(exception, status=400)


class AmbiguousJobError(ValueError):
    pass


@API.exception(AmbiguousJobError)
def ambiguous_job_error(request, exception):
    return error(exception, status=400)


def match(choices, target):
    """
    Matches `target` to one of `choices`.

    Splits the target and each choice into words.  Selects a choice such that
    each word in the target appears as a word in the choice, at least as a
    prefix.

    :return:
      The matching choice.
    """
    REGEX = re.compile(r"[^A-Za-z0-9]")

    def words(target):
        return set(REGEX.split(target))

    target_words = words(target)

    def match(choice):
        choice_words = words(choice)
        return all(
            any( cw.startswith(sw) for cw in choice_words )
            for sw in target_words
        )

    choices = { c for c in choices if match(c) }

    if len(choices) == 0:
        raise JobLookupError("no job id match: " + target)
    elif len(choices) == 1:
        return next(iter(choices))
    else:
        if len(choices) > 8:
            choices = ", ".join(list(choices)[: 8]) + " …"
        else:
            choices = ", ".join(choices)
        raise AmbiguousJobError("ambiguous job id: " + choices)


def match_job_id(jobs, job_id):
    """
    Matches `job_id` as an exact or fuzzy match.
    """
    # Try for an exact match first.a
    try:
        jobs.get_job(job_id)
    except LookupError:
        pass
    else:
        return job_id

    # FIXME: Cache job ids (or word split job ids) to make this efficient.
    job_ids = [ j.job_id for j in jobs.get_jobs(ad_hoc=False) ]
    return match(job_ids, job_id)
 

@API.route("/jobs/<job_id:path>")
async def job(request, job_id):
    jobs = request.app.apsis.jobs
    try:
        job_id = match_job_id(jobs, unquote(job_id))
    except LookupError:
        return error(f"no job_id {job_id}", status=404)
    job = jobs.get_job(job_id)
    return response_json(job_to_jso(request.app, job))



# @API.route("/jobs/<job_id>/runs")
@API.route("/jobs/<job_id:path>/runs")
async def job_runs(request, job_id):
    job_id = match_job_id(request.app.apsis.jobs, unquote(job_id))
    when, runs = request.app.apsis.run_store.query(job_id=job_id)
    jso = runs_to_jso(request.app, when, runs)
    return response_json(jso)


@API.route("/jobs")
async def jobs(request):
    """
    Returns (non ad-hoc) jobs.
    """
    args    = request.args
    try:
        label, = args["label"]
    except KeyError:
        label = None

    jso = [ 
        job_to_jso(request.app, j) 
        for j in request.app.apsis.jobs.get_jobs(ad_hoc=False)
        if label is None or label in j.meta.get("labels")
    ]
    return response_json(jso)


#-------------------------------------------------------------------------------
# Runs

@API.route("/runs/<run_id>", methods={"GET"})
async def run(request, run_id):
    try:
        when, run = request.app.apsis.run_store.get(run_id)
    except KeyError:
        return error(f"unknown run {run_id}", 404)

    jso = runs_to_jso(request.app, when, [run])
    return response_json(jso)


@API.route("/runs/<run_id>/log", methods={"GET"})
async def run_log(request, run_id):
    try:
        run_log = await request.app.apsis.get_run_log(run_id)
    except KeyError:
        return error(f"unknown run {run_id}", 404)

    return response_json({
        "run_log": [
            {
                "run_id"    : r["run_id"],
                "timestamp" : time_to_jso(r["timestamp"]),
                "message"   : r["message"],
            }
            for r in sorted(run_log, key=lambda r: r["timestamp"])
        ]
    })


@API.route("/runs/<run_id>/outputs", methods={"GET"})
async def run_output_meta(request, run_id):
    try:
        outputs = request.app.apsis.outputs.get_metadata(run_id)
    except KeyError:
        log.error(f"unknown run {run_id}", exc_info=True)
        return error(f"unknown run {run_id}", 404)

    jso = _output_metadata_to_jso(request.app, run_id, outputs)
    return response_json(jso)


@API.route("/runs/<run_id>/output/<output_id>", methods={"GET"})
async def run_output(request, run_id, output_id):
    try:
        output = request.app.apsis.outputs.get_output(run_id, output_id)
    except LookupError as exc:
        return error(exc, 404)
    else:
        headers, data = encode_response(
            request.headers, output.data, output.compression)
        headers["Content-Type"] = output.metadata.content_type
        return sanic.response.raw(data, headers=headers)


@API.route("/runs/<run_id>/state", methods={"GET"})
async def run_state_get(request, run_id):
    _, run = request.app.apsis.run_store.get(run_id)
    return response_json({"state": run.state})


@API.route("/runs/<run_id>/skip", methods={"POST"})
async def run_skip(request, run_id):
    state = request.app.apsis
    _, run = state.run_store.get(run_id)
    await state.skip(run)
    return response_json({})


@API.route("/runs/<run_id>/start", methods={"POST"})
async def run_start(request, run_id):
    state = request.app.apsis
    _, run = state.run_store.get(run_id)
    await state.start(run)
    return response_json({})


@API.route("/runs/<run_id>/rerun", methods={"POST"})
async def run_rerun(request, run_id):
    state = request.app.apsis
    _, run = state.run_store.get(run_id)
    new_run = await state.rerun(run)
    jso = runs_to_jso(request.app, ora.now(), [new_run])
    # Let UIs know to show the new run.
    jso["show_run_id"] = new_run.run_id
    return response_json(jso)


@API.route("/runs/<run_id>/riprovall", methods={"POST"})
async def run_riprovall(request, run_id):
    log.info("ZZZZZZZZZZZZZZZZZZZZZZZZZZZ")
    print("YYYYYYYYYYYYYYYYYYYYYYYY")
    jso = {
        "ludo": "23",
        "marco": "24",
    }
    # state = request.app.apsis
    # _, run = state.run_store.get(run_id)
    # new_run = await state.riprovall(run)
    # jso = runs_to_jso(request.app, ora.now(), [new_run])
    # # Let UIs know to show the new run.
    # jso["show_run_id"] = new_run.run_id
    return response_json(jso)


# PUT is probably right, but run actions currently are POST only.
@API.route("/runs/<run_id>/signal/<signal>", methods={"PUT", "POST"})
async def run_signal(request, run_id, signal):
    apsis = request.app.apsis
    _, run = apsis.run_store.get(run_id)

    try:
        signal = to_signal(signal)
    except ValueError:
        return error(f"invalid signal: {signal}", 400)

    try:
        await apsis.send_signal(run, signal)
    except RuntimeError as exc:
        return error(str(exc), 400)
    else:
        return response_json({})


@API.route("/runs/<run_id>/mark/<state>", methods={"PUT", "POST"})
async def run_mark(request, run_id, state):
    try:
        apsis = request.app.apsis
        _, run = apsis.run_store.get(run_id)
        try:
            state = Run.STATE[state]
        except KeyError:
            return error(f"invalid state: {state}", status=400)

        await apsis.mark(run, state)
    except Exception:
        log.error("error", exc_info=True)
        raise
    return response_json({})


def _filter_runs(runs, args):
    """
    Constructs a filter for runs from query args.
    """
    try:
        run_id, = args["run_id"]
    except KeyError:
        pass
    else:
        runs = ( r for r in runs if r.run_id == run_id )

    try:
        job_id, = args["job_id"]
    except KeyError:
        pass
    else:
        runs = ( r for r in runs if r.inst.job_id == job_id )

    return runs


@API.route("/runs")
async def runs(request):
    apsis = request.app.apsis

    # Get runs from the selected interval.
    args        = request.args
    summary,    = args.pop("summary", ("False", ))
    summary     = to_bool(summary)
    run_id      = args.pop("run_id", None)
    job_id,     = args.pop("job_id", (None, ))
    if job_id is not None:
        job_id  = match_job_id(apsis.jobs, job_id)
    state,      = args.pop("state", (None, ))
    since,      = args.pop("since", (None, ))

    # Remainders are args to match, though strip off leading underscores, where
    # were added to avoid collision with fixed args.
    args = { n[1 :] if n.startswith("_") else n: a[-1] for n, a in args.items() }

    when, runs = apsis.run_store.query(
        run_ids     =run_id, 
        job_id      =job_id,
        state       =to_state(state),
        since       =since, 
        with_args   =args,
    )

    return response_json(runs_to_jso(request.app, when, runs, summary=summary))


@API.websocket("/ws/runs")
async def websocket_runs(request, ws):
    since, = request.args.pop("since", (None, ))

    log.info("live runs connect")
    with request.app.apsis.run_store.query_live(since=since) as queue:
        while True:
            # FIXME: If the socket closes, clean up instead of blocking until
            # the next run is available.  Not sure how to do this.  ws.ping()
            # with a timeout doesn't appear to work.
            next_runs = [await queue.get()]
            # Sleep a short while to allow additional runs to enqueue.  This
            # avoids sending lots of short messages to the client.
            await asyncio.sleep(0.5)
            # Drain the queue.
            while True:
                try:
                    next_runs.append(queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            if any( r is None for r in next_runs ):
                # Signalled to shut down.
                await ws.close()
                break

            when = next_runs[-1][0]
            assert all( w <= when for w, _ in next_runs )
            runs = apsis.lib.itr.chain.from_iterable( r for _, r in next_runs )
            runs = _filter_runs(runs, request.args)

            # Break large sets into chunks, to avoid block for too long.
            chunks = list(apsis.lib.itr.chunks(runs, WS_RUN_CHUNK))
            if len(chunks) == 0:
                continue

            try:
                for chunk in chunks:
                    with Timer() as timer:
                        jso = runs_to_jso(request.app, when, chunk, summary=True)
                        # FIXME: JSOs are cached but ujson.dumps() still takes real
                        # time.
                        json = ujson.dumps(jso, escape_forward_slashes=False)
                    log.debug(f"sending {len(chunk)} runs, {len(json)} bytes {timer.elapsed:.3f} s: {request.socket}")
                    await ws.send(json)
                    await asyncio.sleep(WS_RUN_CHUNK_SLEEP)
            except websockets.ConnectionClosed:
                log.info("websocket connection closed")
                break

    log.info("live runs disconnect")


@API.route("/runs", methods={"POST"})
async def run_post(request):
    apsis = request.app.apsis

    # The run may either contain a job ID, or a complete job.
    jso = request.json
    if "job" in jso:
        # A complete job.
        job = jso_to_job(jso["job"], None)
        job.ad_hoc = True
        request.app.apsis.jobs.add(job)
        job_id = job.job_id

    elif "job_id" in jso:
        # Just a job ID.
        job_id = match_job_id(apsis.jobs, jso["job_id"])

    else:
        return error("missing job_id or job")

    run = Run(Instance(job_id, jso.get("args", {})))
    request.app.apsis._validate_run(run)

    time = jso.get("times", {}).get("schedule", "now")
    time = None if time == "now" else ora.Time(time)
    await apsis.schedule(time, run)
    jso = runs_to_jso(request.app, ora.now(), [run])
    return response_json(jso)


# FIXME: Is there a need for this?
@API.route("/runs/reschedule/<job_id:path>", methods={"POST"})
async def runs_reschedule_post(request, job_id):
    await reschedule_runs(request.app.apsis, job_id)
    return response_json({})


