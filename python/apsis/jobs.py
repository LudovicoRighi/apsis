from   contextlib import suppress
import os
from   pathlib import Path
import random
import string
import sys
import yaml

from   .actions import action_from_jso, action_to_jso
from   .lib.py import tupleize
from   .program import program_from_jso, program_to_jso
from   .schedule import schedule_from_jso, schedule_to_jso

#-------------------------------------------------------------------------------

class Reruns:
    """
    :ivar count:
      Maximum number of reruns; 0 for no reruns.
    :ivar delay:
      Delay after failure before rerun.
    :ivar max_delay:
      Maximum delay after schedule time for starting a rerun.  If this delay
      has elapsed, no further reruns are attempted.
    """

    def __init__(self, count=0, delay=0, max_delay=None):
        self.count      = int(count)
        self.delay      = float(delay)
        self.max_delay  = None if max_delay is None else float(max_delay)



class Job:

    def __init__(self, job_id, params, schedules, program, reruns=Reruns(), 
                 actions=[], *, meta={}, ad_hoc=False):
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
        self.reruns     = reruns
        self.actions    = actions
        self.meta       = meta
        self.ad_hoc     = bool(ad_hoc)



#-------------------------------------------------------------------------------

class JobSpecificationError(Exception):

    pass



# FIXME: Do much better at handling errors when converting JSO.

def jso_to_reruns(jso):
    return Reruns(
        count       =jso.get("count", 0),
        delay       =jso.get("delay", 0),
        max_delay   =jso.get("max_delay", None),
    )


def reruns_to_jso(reruns):
    return {
        "count"     : reruns.count,
        "delay"     : reruns.delay,
        "max_delay" : reruns.max_delay,
    }


def jso_to_job(jso, job_id):
    jso = dict(jso)

    assert jso.pop("job_id", job_id) == job_id, f"JSON job_id mismatch {job_id}"

    params = jso.pop("params", [])
    params = [params] if isinstance(params, str) else params

    # FIXME: 'schedules' for backward copmatibility; remove in a while.
    schedules = jso.pop("schedule", jso.pop("schedules", ()))
    schedules = (
        [schedules] if isinstance(schedules, dict) 
        else [] if schedules is None
        else schedules
    )
    schedules = [ schedule_from_jso(s) for s in schedules ]

    try:
        program = jso.pop("program")
    except KeyError:
        raise JobSpecificationError("missing program")
    program = program_from_jso(program)

    actions = jso.pop("actions", [])
    actions = [actions] if isinstance(actions, dict) else actions
    actions = [ action_from_jso(a) for a in actions ]

    reruns      = jso_to_reruns(jso.pop("reruns", {}))
    metadata    = jso.pop("metadata", {})
    ad_hoc      = jso.pop("ad_hoc", False)

    if len(jso) > 0:
        raise JobSpecificationError("unknown keys: " + ", ".join(jso))

    return Job(
        job_id, params, schedules, program, 
        reruns  =reruns, 
        actions =actions,
        meta    =metadata,
        ad_hoc  =ad_hoc,
    )


def job_to_jso(job):
    return {
        "job_id"        : job.job_id,
        "params"        : list(sorted(job.params)),
        "schedule"      : [ schedule_to_jso(s) for s in job.schedules ],
        "program"       : program_to_jso(job.program),
        "reruns"        : reruns_to_jso(job.reruns),
        "metadata"      : job.meta,
        "ad_hoc"        : job.ad_hoc,
    }


def load_yaml(file, job_id):
    jso = yaml.load(file)
    return jso_to_job(jso, job_id)


def load_yaml_files(dir_path):
    dir_path = Path(dir_path)
    for dir, _, names in os.walk(dir_path):
        dir = Path(dir)
        paths = ( dir / n for n in names )
        paths = ( p for p in paths if p.suffix == ".yaml" )
        for path in paths:
            name = path.with_suffix("").relative_to(dir_path)
            with open(path) as file:
                yield load_yaml(file, name)


def check_job_file(path):
    """
    Parses job file at `path`, checks validity, and logs errors.

    :return:
      The job, if successfully loaded and checked, or none.
    """
    path = Path(path)
    job_id = path.with_suffix("").name

    error = lambda msg: print(msg, file=sys.stdout)
    with open(path) as file:
        try:
            jso = yaml.load(file)
        except yaml.YAMLError as exc:
            error(f"failed to parse YAML: {exc}")
            return None

        try:
            job = jso_to_job(jso, job_id)
        except JobSpecificationError as exc:
            error(f"failed to parse job: {exc}")
            return None

        # FIXME: Additional checks here?

        return job


#-------------------------------------------------------------------------------

# FIXME: Rename get_job() -> get().
# FIXME: Rename get_jobs() -> all().

class JobsDir:

    # FIXME: Mapping API?

    def __init__(self, path):
        self.__path = Path(path)
        # FIXME: Detect duplicates.
        self.__jobs = {
            job.job_id: job
            for job in load_yaml_files(path)
        }


    def get_job(self, job_id) -> Job:
        """
        :raise LookupError:
          Can't find `job_id`.
        """
        try:
            return self.__jobs[job_id]
        except KeyError:
            raise LookupError(job_id)


    def get_jobs(self, *, ad_hoc=None):
        jobs = self.__jobs.values()
        if ad_hoc is not None:
            jobs = ( j for j in jobs if j.ad_hoc == ad_hoc )
        return jobs



#-------------------------------------------------------------------------------

# FIXME: This feels so awkward.  Is there a better design?

class Jobs:
    """
    Combines a job dir and a job DB.
    """

    def __init__(self, job_dir, job_db):
        self.__job_dir = job_dir
        self.__job_db = job_db


    def get_job(self, job_id) -> Job:
        with suppress(LookupError):
            return self.__job_dir.get_job(job_id)
        return self.__job_db.get(job_id)


    def get_jobs(self, *, ad_hoc=None):
        """
        :param ad_hoc:
          If true, return ad hoc jobs only; if false, return normal jobs only;
          if none, return all jobs.
        """
        if ad_hoc is None or not ad_hoc:
            yield from self.__job_dir.get_jobs()
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
                


