from   contextlib import suppress
import logging
import ora
import os
from   pathlib import Path
import random
import string
import yaml
from   collections import defaultdict

from   .actions import Action
from   .actions.schedule import successor_from_jso
from   .cond import Condition
from   .lib.json import to_array, check_schema
from   .lib.py import tupleize, format_ctor
from   .program import Program, NoOpProgram
from   .schedule import Schedule
from   apsis.lib.exc import SchemaError

log = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

class Job:

    def __init__(
            self, job_id, params=[], schedules=[], program=NoOpProgram(),
            conds=[], actions=[], *, meta={}, ad_hoc=False, downs=[]
    ):
        """
        :param schedules:
          A sequence of `Schedule, args` pairs, where `args` is an arguments
          dict.
        :param ad_hoc:
          True if this is an ad hoc job.
        :param meta:
          Dict of metadata.  Must be JSON-serlializable.
        """
        self.job_id     = None if job_id is None else str(job_id)
        self.params     = frozenset( str(p) for p in tupleize(params) )
        self.schedules  = tupleize(schedules)
        self.program    = program
        self.conds      = tupleize(conds)
        self.downs      = tupleize(downs)
        self.actions    = actions
        self.meta       = meta
        self.ad_hoc     = bool(ad_hoc)


    def __repr__(self):
        return format_ctor(
            self, self.job_id, tuple(self.params),
            schedules   =self.schedules,
            program     =self.program,
            conds       =self.conds,
            actions     =self.actions,
            meta        =self.meta,
            ad_hoc      =self.ad_hoc,
        )


    def __eq__(self, other):
        return (
                not self.ad_hoc
            and not other.ad_hoc
            and other.params    == self.params
            and other.schedules == self.schedules
            and other.program   == self.program
            and other.conds     == self.conds
            and other.actions   == self.actions
            and other.meta      == self.meta
        )



#-------------------------------------------------------------------------------

class JobErrors(Exception):
    """
    One or more exceptions.
    """

    def __init__(self, msg, errors):
        super().__init__(msg)
        self.errors = tuple(errors)


    def format(self):
        for error in self.errors:
            yield f"{error.job_id}: {error}"



# FIXME: Do much better at handling errors when converting JSO.

def jso_to_job(jso, job_id):
    with check_schema(jso) as pop:
        # FIXME: job_id here at all?
        assert pop("job_id", default=job_id) == job_id, f"JSON job_id mismatch {job_id}"

        params      = pop("params", default=[])
        params      = [params] if isinstance(params, str) else params

        # FIXME: 'schedules' for backward compatibility; remove in a while.
        schedules   = pop("schedule", default=())
        schedules   = (
            [schedules] if isinstance(schedules, dict) 
            else [] if schedules is None
            else schedules
        )
        schedules   = [ Schedule.from_jso(s) for s in schedules ]

        program     = pop("program", Program.from_jso)

        conds       = pop("condition", to_array, default=[])
        conds       = [ Condition.from_jso(c) for c in conds ]

        acts        = pop("action", to_array, default=[])
        acts        = [ Action.from_jso(a) for a in acts ]

        # Successors are syntactic sugar for actions.
        sucs        = pop("successors", to_array, default=[])
        acts.extend([ successor_from_jso(s) for s in sucs ])

        metadata    = pop("metadata", default={})
        metadata["labels"] = [
            str(l)
            for l in tupleize(metadata.get("labels", []))
        ]

        ad_hoc      = pop("ad_hoc", bool, default=False)

    return Job(
        job_id, params, schedules, program,
        conds   =conds,
        downs=[],
        actions =acts,
        meta    =metadata,
        ad_hoc  =ad_hoc,
    )


def job_to_jso(job):
    return {
        "job_id"        : job.job_id,
        "params"        : list(sorted(job.params)),
        "schedule"      : [ s.to_jso() for s in job.schedules ],
        "program"       : job.program.to_jso(),
        "condition"     : [ c.to_jso() for c in job.conds ],
        "action"        : [ a.to_jso() for a in job.actions ],
        "metadata"      : job.meta,
        "ad_hoc"        : job.ad_hoc,
    }


def load_yaml(file, job_id):
    jso = yaml.load(file, Loader=yaml.SafeLoader)
    return jso_to_job(jso, job_id)


def load_yaml_file(path, job_id):
    with open(path) as file:
        return load_yaml(file, job_id)


def list_yaml_files(dir_path):
    dir_path = Path(dir_path)
    for dir, _, names in os.walk(dir_path):
        dir = Path(dir)
        paths = ( dir / n for n in names if not n.startswith(".") )
        paths = ( p for p in paths if p.suffix == ".yaml" )
        for path in paths:
            job_id = str(path.with_suffix("").relative_to(dir_path))
            yield path, job_id


#-------------------------------------------------------------------------------

# FIXME: Use mapping API for jobs.

class InMemoryJobs:
    """
    In-memory set of jobs.  Used for testing.
    """

    def __init__(self, jobs):
        self.__jobs = { j.job_id: j for j in jobs }


    def get_job(self, job_id) -> Job:
        try:
            return self.__jobs[job_id]
        except KeyError:
            raise LookupError(f"no job {job_id}")


    def get_jobs(self, *, ad_hoc=None):
        jobs = self.__jobs.values()
        if ad_hoc is not None:
            jobs = ( j for j in jobs if j.ad_hoc == ad_hoc )
        return jobs



class JobsDir:

    def __init__(self, path, jobs):
        self.__path = path
        self.__jobs = jobs


    def __repr__(self):
        return format_ctor(self, str(self.__path))


    @property
    def path(self):
        return self.__path


    def get_job(self, job_id) -> Job:
        """
        :raise LookupError:
          Can't find `job_id`.
        """
        try:
            return self.__jobs[job_id]
        except KeyError:
            raise LookupError(f"no job {job_id}")


    def get_jobs(self, *, ad_hoc=None):
        jobs = self.__jobs.values()
        if ad_hoc is not None:
            jobs = ( j for j in jobs if j.ad_hoc == ad_hoc )
        return jobs



def load_jobs_dir(path):
    """
    Attempts to loads jobs from a jobs dir.

    :return:
      The successfully loaded `JobsDir`.
    :raise JobErrors:
      One or more errors while loading jobs.  The exception's `errors` attribute
      contains the errors; each has a `job_id` attribute.
    """
    jobs_path = Path(path)
    jobs = {}
    errors = []
    for path, job_id in list_yaml_files(jobs_path):
        log.debug(f"loading: {path}")
        try:
            jobs[job_id] = load_yaml_file(path, job_id)
        except SchemaError as exc:
            log.debug(f"error: {path}: {exc}", exc_info=True)
            exc.job_id = job_id
            errors.append(exc)

    # TODO: not very clear what to consider as dependency here (i.e. which states to use)
    # log.warning(c.states)    
    downstreams = defaultdict(list)
    for job_id, spec in jobs.items():
        # log.warning(job_id)
        # log.warning("- - - - - - - - - - - ")
        for c in spec.conds:
            # log.warning(c.job_id)
            # log.error(jobs[c.job_id].downs)
            downstreams[c.job_id].append(job_id)

        # log.warning("---------------------------------------------------")

    for j_id, dds in downstreams.items():
        log.warning(f"THE FOLLOWING IS THE LIST OF JOBS THAT ARE DOWNSTREAMS WITH RESPECT TO: {j_id}:")
        log.warning(dds)
    
    for job_id in jobs.keys():
        jobs[job_id].downs = tupleize(downstreams[job_id])

    if len(errors) > 0:
        raise JobErrors(f"errors loading jobs in {jobs_path}", errors)
    else:
        return JobsDir(jobs_path, jobs)


def check_job(jobs_dir, job):
    """
    Performs consistency checks on `job` in `jobs_dir`.

    :return:
      Generator of errors.
    """
    def check_associated_run(obj, context):
        try:
            associated_job_id = obj.job_id
        except AttributeError:
            # No associated job; that's OK.
            return

        # Find the associated job.
        try:
            associated_job = jobs_dir.get_job(associated_job_id)
        except LookupError:
            yield f"unknown job ID in {context}: {associated_job_id}"
            return

        # Look up additional args, if any.
        try:
            args = set(obj.args)
        except AttributeError:
            args = set()

        params = set(associated_job.params)
        # Check for missing args.  The params of the associated job can be bound
        # either to the args of this job or to explicit args.
        for missing in params - set(job.params) - args:
            yield f"missing arg in {context}: param {missing} of job {associated_job_id}"
        # Check for extraneous explicit args.
        for extra in args - params:
            yield f"extraneous arg in {context}: param {extra} of job {associated_job_id}"

    # Check all job ids in actions and conditions, by checking each action
    # and condition class for a job_id attribute.
    for action in job.actions:
        yield from check_associated_run(action, "action")
    for cond in job.conds:
        yield from check_associated_run(cond, "condition")

    # Try scheduling a run for each schedule of each job.
    now = ora.now()
    for schedule in job.schedules:
        _, args = next(schedule(now))
        missing_args = set(job.params) - set(args)
        if len(missing_args) > 0:
            yield(f"job params missing in schedule args: {' '.join(missing_args)}")


def check_job_dir(path):
    """
    Loads jobs in dir at `path` and checks validity.

    :return:
      Generator of errors.
    """
    if not Path(path).is_dir():
        raise NotADirectoryError(f"not a directory: {path}")

    try:
        jobs_dir = load_jobs_dir(path)
    except JobErrors as exc:
        for err in exc.errors:
            yield f"{err.job_id}: {err}"
        return

    for job in jobs_dir.get_jobs():
        log.info(f"checking: {job.job_id}")
        for err in check_job(jobs_dir, job):
            yield f"{job.job_id}: {err}"


#-------------------------------------------------------------------------------

# FIXME: This feels so awkward.  Is there a better design?

class Jobs:
    """
    Combines a job dir and a job DB.
    """

    def __init__(self, jobs_dir, job_db):
        self.__jobs_dir = jobs_dir
        self.__job_db = job_db


    def get_job(self, job_id) -> Job:
        with suppress(LookupError):
            return self.__jobs_dir.get_job(job_id)
        return self.__job_db.get(job_id)


    __getitem__ = get_job


    def get_jobs(self, *, ad_hoc=None):
        """
        :param ad_hoc:
          If true, return ad hoc jobs only; if false, return normal jobs only;
          if none, return all jobs.
        """
        if ad_hoc is None or not ad_hoc:
            yield from self.__jobs_dir.get_jobs()
        # FIXME: Yield only job ids we haven't seen.
        yield from self.__job_db.query(ad_hoc=ad_hoc)


    def __get_job_id(self):
        # FIXME: Something better.
        return (
            "adhoc-"
            + "".join( random.choice(string.ascii_letters) for _ in range(12) )
        )


    def add(self, job):
        assert job.job_id is None
        job.job_id = self.__get_job_id()

        self.__job_db.insert(job)



def diff_jobs_dirs(jobs_dir0, jobs_dir1):
    """
    Finds differences between job dirs.

    :return:
      Job IDs that have been removed, job IDs that have been added, and job IDs
      that have changed.
    """
    jobs0 = { j.job_id: j for j in jobs_dir0.get_jobs(ad_hoc=False) }
    jobs1 = { j.job_id: j for j in jobs_dir1.get_jobs(ad_hoc=False) }
    job_ids0 = frozenset(jobs0)
    job_ids1 = frozenset(jobs1)
    ids = job_ids0 & job_ids1
    return (
        job_ids0 - job_ids1,
        job_ids1 - job_ids0,
        frozenset( i for i in ids if jobs1[i] != jobs0[i] ),
    )


