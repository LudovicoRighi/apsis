import asyncio
import argparse
from   cron import *
from   functools import partial
import logging
import sanic
import sanic.response
import time
import websockets

from   apsis import api, crontab, job, scheduler
from   apsis.state import state
import apsis.testing

#-------------------------------------------------------------------------------

LOG_FORMATTER = logging.Formatter(
    fmt="%(asctime)s %(name)-16s [%(levelname)-7s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ"
)
LOG_FORMATTER.converter = time.gmtime  # FIXME: Use cron.Time?

#-------------------------------------------------------------------------------

class QueueHandler(logging.Handler):
    """
    Publishes formatted log messages to registered async queues.
    """

    def __init__(self, formatter=None):
        if formatter is None:
            formatter = logging.Formatter()

        super().__init__()
        self.__formatter = formatter
        self.__queues = []


    def register(self) -> asyncio.Queue:
        """
        Returns a new queue, to which log records will be published.
        """
        queue = asyncio.Queue()
        self.__queues.append(queue)
        return queue


    def unregister(self, queue):
        """
        Removes a previously registered queue.
        """
        self.__queues.remove(queue)


    def emit(self, record):
        data = self.__formatter.format(record)
        for queue in list(self.__queues):
            try:
                queue.put_nowait(data)
            except asyncio.QueueFull:
                pass


WS_HANDLER = QueueHandler(LOG_FORMATTER)

#-------------------------------------------------------------------------------

app = sanic.Sanic(__name__, log_config=None)
app.config.LOGO = None

app.static("/static", "./static")
app.blueprint(api.API, url_prefix="/api/v1")

@app.websocket("/log")
async def websocket_log(request, ws):
    queue = WS_HANDLER.register()
    try:
        while True:
            try:
                await ws.send(await queue.get())
            except websockets.ConnectionClosed:
                break
    finally:
        WS_HANDLER.unregister(queue)


#-------------------------------------------------------------------------------

def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().handlers[0].formatter = LOG_FORMATTER
    logging.getLogger().handlers.append(WS_HANDLER)
    logging.getLogger("asyncio").setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug", action="store_true", default=False,
        help="run in debug mode")
    parser.add_argument(
        "--host", metavar="HOST", default="localhost",
        help="server host address")
    parser.add_argument(
        "--port", metavar="PORT", type=int, default=5000,
        help="server port")
    # parser.add_argument(
    #     "job_dir", metavar="JOBDIR", 
    #     help="job directory")
    parser.add_argument(
        "crontab", metavar="CRONTAB",
        help="crontab file")
    args = parser.parse_args()

    # for j in job.load_job_dir(args.job_dir):
    #     state.add_job(j)
    environment, jobs = crontab.read_crontab_file(args.crontab)
    for name, val in environment.items():
        print("{} = {}".format(name, val))
    for job in jobs:
        state.add_job(job)

    time = now()
    docket = scheduler.Docket(time)
    scheduler.schedule_insts(docket, time + 1 * 86400)

    loop = asyncio.get_event_loop()

    # Set off the recurring handler.
    loop.call_soon(scheduler.docket_handler, docket)

    server = app.create_server(
        host        =args.host,
        port        =args.port,
        debug       =args.debug,
        log_config  =None,
    )
    app.running = True
    asyncio.ensure_future(server, loop=loop)

    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    main()

