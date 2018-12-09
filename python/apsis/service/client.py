import logging
from   ora import Time
import requests
from   urllib.parse import quote, urlunparse

import apsis.service

#-------------------------------------------------------------------------------

class APIError(RuntimeError):

    def __init__(self, status, error):
        super().__init__(f"{error} [API status {status}]")
        self.status = status



class Client:

    def __init__(self, host, port=apsis.service.DEFAULT_PORT):
        self.__host = host
        self.__port = port


    def __url(self, *path, **query):
        query = "&".join(
            f"{k}={quote(str(v))}"
            for k, v in query.items()
            if v is not None
        )
        return urlunparse((
            "http",
            f"{self.__host}:{self.__port}",
            "/".join(path),
            "",
            query,
            "",
        ))


    def __request(self, method, *path, data=None, **query):
        url = self.__url(*path, **query)
        logging.debug(f"{method} {url} data={data}")
        resp = requests.request(method, url, json=data)
        if 200 <= resp.status_code < 300:
            return resp.json()
        else:
            try:
                error = resp.json()["error"]
            except Exception:
                error = "unknown error"
            raise APIError(resp.status_code, error)


    def __get(self, *path, **query):
        return self.__request("GET", *path, **query)


    def __post(self, *path, data):
        return self.__request("POST", *path, data=data)


    def get_job(self, job_id):
        return self.__get("/api/v1/jobs", job_id)


    def get_job_runs(self, job_id):
        return self.__get("/api/v1/jobs", job_id, "runs")["runs"]


    def get_jobs(self):
        return self.__get("/api/v1/jobs")


    def get_output(self, run_id, output_id) -> bytes:
        url = self.__url("/api/v1/runs", run_id, "output", output_id)
        logging.debug(f"GET {url}")
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.content


    def get_runs(self, *, job_id=None, state=None, reruns=False,
                 since=None, until=None):
        return self.__get(
            "/api/v1/runs",
            job_id  =job_id,
            state   =state,
            reruns  =reruns,
        )["runs"]


    def get_run(self, run_id):
        return self.__get("/api/v1/runs", run_id)["runs"][run_id]


    def rerun(self, run_id):
        run, = self.__post("/api/v1/runs", run_id, "rerun", data={})["runs"].values()
        return run


    def schedule(self, job_id, args, time):
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
        

    def shut_down(self):
        self.__post("/api/control/shut_down", data={})



