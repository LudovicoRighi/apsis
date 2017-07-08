import logging
import sanic

from   .state import state

log = logging.getLogger("api/v1")

#-------------------------------------------------------------------------------

API = sanic.Blueprint("v1")

def json(jso):
    return sanic.response.json(jso, indent=1, sort_keys=True)


#-------------------------------------------------------------------------------
# Jobs

def job_to_jso(app, job):
    jso = job.to_jso()
    jso["url"] = app.url_for("v1.job", job_id=job.job_id)
    return jso


@API.route("/jobs/<job_id>")
async def job(request, job_id):
    jso = state.get_job(job_id).to_jso()
    return json(jso)


@API.route("/jobs")
async def jobs(request):
    jso = [ 
        job_to_jso(request.app, j) 
        for j in state.get_jobs() 
    ]
    return json(jso)


#-------------------------------------------------------------------------------
# Results

def result_to_jso(app, result):
    jso = result.to_jso(full=False)
    jso.update({
        "url"       : app.url_for("v1.result", run_id=result.run.run_id),
        # FIXME: "run_url"
        # FIXME: "inst_url"
        "job_url"   : app.url_for("v1.job", job_id=result.run.inst.job.job_id),
        "output_url": app.url_for("v1.result_output", run_id=result.run.run_id),
    })
    return jso


@API.route("/results/<run_id>")
async def result(request, run_id):
    jso = result_to_jso(request.app, state.get_result(run_id))
    return json(jso)


@API.route("/results/<run_id>/output")
async def result_output(request, run_id):
    jso = state.get_result(run_id).output  # FIXME: to_jso
    return json(jso)


@API.route("/results")
async def results(request):
    jso = [ 
        result_to_jso(request.app, r) 
        for r in state.get_results() 
    ]
    return json(jso)


