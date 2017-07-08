from   aslib.py import format_ctor
import json
from   pathlib import Path
from   typing import *

#-------------------------------------------------------------------------------

class Job:

    def __init__(self, job_id, schedule, program):
        self.job_id     = job_id
        self.schedule   = schedule
        self.program    = program


    def to_jso(self):
        from . import program, schedule
        return {
            "job_id"    : self.job_id,
            "schedule"  : schedule.to_jso(self.schedule),
            "program"   : program.to_jso(self.program),
        }


    @classmethod
    def from_jso(class_, jso):
        from . import program, schedule
        return class_(
            jso["$id"],
            schedule.from_jso(jso["schedule"]),
            program.from_jso(jso["program"]),
        )



class Instance:

    def __init__(self, id, job, time):
        self.__id       = id
        self.__job      = job
        self.__time     = time


    def __repr__(self):
        return format_ctor(self, self.__id, self.__job, self.__time)


    def __lt__(self, other):
        return (
            self.__id < other.__id if isinstance(other, Instance)
            else NotSupported
        )


    @property
    def id(self):
        return self.__id


    @property
    def job(self):
        return self.__job


    @property
    def time(self):
        return self.__time


    def to_jso(self):
        return {
            "$id"       : self.__id,
            "job"       : self.__job.to_jso(),
            "time"      : str(self.__time),
        }



class Run:

    def __init__(self, run_id, inst):
        self.run_id = run_id
        self.inst   = inst


    def __repr__(self):
        return format_ctor(self, self.run_id)


    def __str__(self):
        return "run {} of {}".format(self.run_id, self.inst)


    def to_jso(self):
        return {
            "run_id"    : self.run_id,
            "inst"      : self.inst.to_jso(),
        }



#-------------------------------------------------------------------------------

def load_job_file_json(path: Path) -> Job:
    with open(path) as file:
        jso = json.load(file)
    jso.setdefault("$id", path.with_suffix("").name)
    return Job.from_jso(jso)


def load_job_dir(path: Path) -> Iterable[Job]:
    """
    Loads job files in `path`.

    Skips files that do not have a supported extension.
    """
    path = Path(path)
    for entry in path.iterdir():
        if entry.suffix == ".json":
            yield load_job_file_json(entry)
        else:
            # Skip it.
            pass


