import logging
from   ora import Time
import os
from   collections import namedtuple
import requests
from   urllib.parse import quote, urlunparse

import apsis.service

#-------------------------------------------------------------------------------

Address = namedtuple("Address", ("host", "port"))

def get_address() -> Address:
    # TODO: RESTORE THIS FUNCTION, IT SHOULD NOT BE MODIFIED AT ALL
    return Address(host="rd10.asd", port=5245)



class APIError(RuntimeError):

    def __init__(self, status, error, jso):
        super().__init__(error)
        self.status = status
        self.jso = jso



NO_ARG = object()

class Client:

    def __init__(self, address=None):
        """
        :param addr:
          Apsis hostname and port, or none for default.
        """
        self.__addr = get_address() if address is None else Address(*address)


    def __url(self, *path, **query):
        query = "&".join(
            str(k) if v is NO_ARG else f"{k}={quote(str(v))}"
            for k, v in query.items()
            if v is not None
        )
        return urlunparse((
            "http",
            f"{self.__addr.host}:{self.__addr.port}",
            "/".join(path),
            "",
            query,
            "",
        ))


    def __request(self, method, *path, timeout=None, data=None, **query):
        url = self.__url(*path, **query)
        logging.debug(f"{method} {url} data={data}")
        resp = requests.request(method, url, json=data, timeout=timeout)
        if 200 <= resp.status_code < 300:
            return resp.json()
        else:
            try:
                jso = resp.json()
                error = jso.get("error", "unknown error")
            except Exception:
                jso = {}
                error = "unknown error"
            raise APIError(resp.status_code, error, jso)


    def __get(self, *path, **query):
        return self.__request("GET", *path, **query)


    def __post(self, *path, data=None, **query):
        return self.__request("POST", *path, data=data, **query)


    def __put(self, *path, data=None, **query):
        return self.__request("PUT", *path, data=data, **query)


    def alive(self, timeout):
        """
        Checks liveness of the service.

        :param timeout:
          Timeout in sec.
        """
        _ = self.__get("/api/v1/alive", timeout=timeout)


    def stats(self):
        return self.__get("/api/v1/stats")


    def skip(self, run_id):
        """
        Skips a scheduled or waiting run.
        """
        return self.__post("/api/v1/runs", run_id, "skip")


    def start(self, run_id):
        """
        Forces a scheduled or waiting run to start.
        """
        return self.__post("/api/v1/runs", run_id, "start")


    def get_run_log(self, run_id):
        return self.__get("/api/v1/runs", run_id, "log")["run_log"]


    def get_job(self, job_id):
        return self.__get("/api/v1/jobs", job_id)


    def get_job_runs(self, job_id):
        return self.__get("/api/v1/jobs", job_id, "runs")["runs"]


    def get_jobs(self, *, label=None):
        return self.__get("/api/v1/jobs", label=label)


    def get_outputs(self, run_id):
        """
        Returns output metadata for `run_id`.
        """
        url = self.__url("/api/v1/runs", run_id, "outputs")
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json()


    def get_output(self, run_id, output_id) -> bytes:
        url = self.__url("/api/v1/runs", run_id, "output", output_id)
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content


    def signal(self, run_id, signal):
        """
        Sends `signal` to a running processes.
        """
        signal = getattr(signal, "name", str(signal))
        return self.__put("/api/v1/runs", run_id, "signal", signal)


    def mark(self, run_id, state_name):
        """
        Marks a run to different finished state.
        """
        return self.__post("/api/v1/runs", run_id, "mark", state_name)


    def get_runs(self, *, job_id=None, state=None, args={}):
        return self.__get(
            "/api/v1/runs",
            job_id  =job_id,
            state   =state,
            # Include args, but prefix with underscore any that collide with
            # fixed arg names.
            # FIXME: Oh so hacky.
            **{
                "_" + n if n in {"job_id", "run_id", "state", "since"} else n: a
                for n, a in args.items()
            },
        )["runs"]


    def get_run(self, run_id):
        return self.__get("/api/v1/runs", run_id)["runs"][run_id]


    def rerun(self, run_id):
        run, = self.__post("/api/v1/runs", run_id, "rerun", data={})["runs"].values()
        return run
    
    def riprovall(self, run_id):
        print("BBBBBBBBBBBBBBBBBBBBBBB")
        run, = self.__post("/api/v1/runs", run_id, "riprovall", data={})["runs"].values()
        return run

    def schedule(self, job_id, args, time="now"):
        """
        Creates and schedules a new run.
        """
        job_id  = str(job_id)
        args    = { str(k): str(v) for k, v in args.items() }
        time    = "now" if time == "now" else str(Time(time))

        data = {
            "job_id": job_id,
            "args": args,
            "times": {
                "schedule": time,
            }
        }
        runs = self.__post("/api/v1/runs", data=data)["runs"]
        return next(iter(runs.values()))


    def __schedule(self, time, job):
        time = "now" if time == "now" else str(Time(time))
        data = {
            "job": job,
            "times": {
                "schedule": time,
            },
        }
        runs = self.__post("/api/v1/runs", data=data)["runs"]
        return next(iter(runs.values()))


    def schedule_adhoc(self, time, job):
        return self.__schedule(time, job)


    def schedule_program(self, time, args):
        """
        :param time:
          The schedule time, or "now" for immediate.
        :param args:
          The argument vector.  The first item is the path to the program
          to run.
        """
        args = [ str(a) for a in args ]
        return self.__schedule(time, {"program": args})


    def schedule_shell_program(self, time, command):
        """
        :param time:
          The schedule time, or "now" for immediate.
        :param command:
          The shell command to run.
        """
        return self.__schedule(time, {"program": str(command)})


    def reload_jobs(self, *, dry_run=False):
        return self.__post("/api/control/reload_jobs", data={}, dry_run=dry_run)


    def shut_down(self, restart=False):
        query = {"restart": NO_ARG} if restart else {}
        self.__post("/api/control/shut_down", data={}, **query)


    def version(self):
        """
        Returns a version info structure.
        """
        return self.__get("/api/control/version")



