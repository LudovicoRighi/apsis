"""
Main user CLI.
"""

import json
import logging
from   ora import now
import random
import sys
import yaml

import apsis.cmdline
import apsis.jobs
import apsis.lib.argparse
import apsis.lib.itr
import apsis.lib.logging
from   apsis.runs import Run
import apsis.service.client

log = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

def main():
    apsis.lib.logging.rich_configure()

    #-------------------------------------------------------------

    def add_dump_format_option(parser):
        parser.add_argument(
            "--format", metavar="FMT", default=None, choices={"json", "yaml"},
            help="write as FMT [json, yaml]")


    def dump_format(obj, format):
        if format == "json":
            json.dump(obj, sys.stdout, indent=2)
        elif format == "yaml":
            yaml.dump(obj, sys.stdout)


    #-------------------------------------------------------------
    # top-level argument parser

    parser = apsis.lib.argparse.CommandArgumentParser(prog="apsis")
    addr = apsis.service.client.get_address()
    parser.add_argument(
        "--host", metavar="HOST", default=addr.host,
        help=f"connect to HOST [def: {addr.host}]")
    parser.add_argument(
        "--port", metavar="PORT", default=addr.port,
        help=f"connect to PORT [def: {addr.port}]")

    #--- command: adhoc ----------------------------------------------

    def cmd_adhoc(client, args):
        time = apsis.cmdline.parse_at_time(args.time)
        if args.shell:
            command = " ".join(args.command)
            run = client.schedule_shell_program(time, command)
        else:
            run = client.schedule_program(time, args.command)
        apsis.cmdline.print_run(run, con)


    cmd = parser.add_command(
        "adhoc", cmd_adhoc,
        description="Creates and schedules an ad hoc run.")
    cmd.add_argument(
        "time", metavar="TIME",
        help="time to run [time, daytime, 'now']")
    cmd.add_argument(
        "command", metavar="CMD...", nargs="+",
        help="command to run")
    cmd.add_argument(
        "--shell", action="store_true", default=False,
        help="treat CMD as shell code (contactenated)")

    #--- command: cancel ---------------------------------------------

    def cmd_skip(client, args):
        for run_id in args.run_id:
            client.skip(run_id)


    cmd = parser.add_command(
        "skip", cmd_skip,
        description="Skips a scheduled or waiting run.")
    cmd.add_argument(
        "run_id", metavar="RUN-ID ...", nargs="+")

    #--- command: job ------------------------------------------------

    def cmd_job(client, args):
        job_id = args.job  # FIXME
        job = client.get_job(job_id)
        if args.format is None:
            apsis.cmdline.print_job(job, con)
        else:
            dump_format(job, args.format)


    cmd = parser.add_command(
        "job", cmd_job,
        description="Displays a job.")
    cmd.add_argument(
        "job", metavar="JOB-ID",
        help="display job with JOB-ID")
    add_dump_format_option(cmd)

    #--- command: jobs -----------------------------------------------

    def cmd_jobs(client, args):
        jobs = client.get_jobs(label=args.label)
        if args.format is None:
            apsis.cmdline.print_jobs(jobs, con)
            con.print()
        else:
            dump_format(jobs, args.format)


    cmd = parser.add_command(
        "jobs", cmd_jobs,
        description="Lists all jobs.")
    cmd.add_argument(
        "--label", metavar="LABEL", default=None,
        help="List jobs with LABEL.")
    add_dump_format_option(cmd)

    #--- command: mark -----------------------------------------------

    def cmd_mark(client, args):
        for run_id in args.run_id:
            client.mark(run_id, args.state)


    cmd = parser.add_command(
        "mark", cmd_mark,
        description="Marks a run to a different finished STATE.")
    cmd.add_argument(
        "state", metavar="STATE", type=apsis.cmdline.match_state,
        choices={"success", "failure", "error"})
    cmd.add_argument(
        "run_id", metavar="RUN-ID ...", nargs="+")

    #--- command: output ---------------------------------------------

    def cmd_output(client, args):
        # FIXME: For now, expose output_id=output only.
        output = client.get_output(args.run_id, "output")
        sys.stdout.buffer.write(output)


    cmd = parser.add_command(
        "output", cmd_output,
        description="Dumps the output of a run.")
    cmd.add_argument(
        "run_id", metavar="RUN-ID")

    #--- command: rerun ----------------------------------------------

    def cmd_rerun(client, arg):
        runs = [ client.rerun(r) for r in arg.run_id ]
        for run in runs:
            if args.format is None:
                apsis.cmdline.print_run(run, con)
            else:
                dump_format(run, args.format)


    cmd = parser.add_command(
        "rerun", cmd_rerun,
        description="Reruns a failed (or error) run.")
    cmd.add_argument(
        "run_id", metavar="RUN-ID ...", nargs="+")
    add_dump_format_option(cmd)


    #--- command: riprovall ----------------------------------------------

    def cmd_riprovall(client, arg):
        print(f"{client._Client__url()=}")
        print(f"{client.riprovall('129')}")

        for r in arg.run_id:
            print(f"{r=}")
        #     client.riprovall(r)
        # for run in runs:
        #     if args.format is None:
        #         apsis.cmdline.print_run(run, con)
        #     else:
        #         dump_format(run, args.format)

    cmd = parser.add_command(
        "riprovall", cmd_riprovall,
        description="Marks as failed and reruns a run together with all its downstream dependencies.")
    cmd.add_argument(
        "run_id", metavar="RUN-ID ...", nargs="+")
    add_dump_format_option(cmd)


    #--- command: run ------------------------------------------------

    def cmd_run(client, args):
        for run_id in args.run_id:
            run = client.get_run(run_id)
            if args.format is None:
                if args.verbosity > 0:
                    run_log = client.get_run_log(run_id)
                    similar_runs = client.get_runs(job_id=run["job_id"], args=run["args"])
                else:
                    run_log = similar_runs = None
                apsis.cmdline.print_run(
                    run, con, verbosity=args.verbosity,
                    run_log=run_log,
                    similar_runs=similar_runs
                )
            else:
                dump_format(run, args.format)


    cmd = parser.add_command(
        "run", cmd_run,
        description="Displays a run.")
    cmd.add_argument(
        "run_id", metavar="RUN-ID", nargs="+")
    cmd.add_argument(
        "-v", "--verbose", action="count", dest="verbosity", default=0)
    add_dump_format_option(cmd)

    #--- command: runs -----------------------------------------------

    def cmd_runs(client, arg):
        runs = client.get_runs(
            job_id  =args.job,
            state   =args.state,
            # FIXME: times
        )

        if args.summary:
            for run in runs.values():
                apsis.cmdline.print_run(run, con)
        elif args.format is None:
            apsis.cmdline.print_runs(
                runs, con,
                arg_style="separate" if args.job is not None else "combined"
            )
        else:
            dump_format(runs, args.format)


    cmd = parser.add_command(
        "runs", cmd_runs,
        description="Queries and displays runs.")
    cmd.add_argument(
        "--job", "-j", metavar="JOB-ID", default=None,
        help="show only runs of job JOB-ID")
    cmd.add_argument(
        "--state", "-s", metavar="STATE", default=None,
        choices=[ r.name for r in Run.STATE ],
        help="show only runs in STATE")
    cmd.add_argument(
        "--times", "-t", metavar="TIMESPAN", default=None,
        help="show only runs in TIMESPAN")

    grp = cmd.add_mutually_exclusive_group()
    grp.add_argument(
        "--summary", action="store_true", default=False,
        help="summarize each run")
    add_dump_format_option(grp)

    #--- command: schedule -------------------------------------------

    def cmd_schedule(client, args):
        for _ in range(args.count):
            run = client.schedule(args.job_id, dict(args.args), args.time)
            apsis.cmdline.print_run(run, con)


    def parse_arg(arg):
        name, value = arg.split("=", 1)
        return name, value


    cmd = parser.add_command(
        "schedule", cmd_schedule,
        description="Creates and schedules a run.")
    cmd.add_argument(
        "--count", metavar="NUM", type=int, default=1,
        help="schedule NUM runs [def: 1]")
    cmd.add_argument(
        "time", metavar="TIME", type=apsis.cmdline.parse_at_time,
        help="time to run [time, daytime, 'now']")
    cmd.add_argument(
        "job_id", metavar="JOB-ID",
        help="run an instance of JOB-ID")
    cmd.add_argument(
        "args", metavar="NAME=VAL", type=parse_arg, nargs="*",
        help="run JOB-ID with NAME=VAL")

    #--- command: signal ---------------------------------------------

    def cmd_signal(client, args):
        for run_id in args.run_id:
            client.signal(run_id, args.signum.upper())


    cmd = parser.add_command(
        "signal", cmd_signal,
        description="Sends a signal to a running run.")
    cmd.add_argument(
        "signum", metavar="SIGNAL", default="SIGTERM",
        help="signal name or number")
    cmd.add_argument(
        "run_id", metavar="RUN-ID ...", nargs="+")

    #--- command: start ----------------------------------------------

    def cmd_start(client, args):
        for run_id in args.run_id:
            client.start(run_id)


    cmd = parser.add_command(
        "start", cmd_start,
        description="Forces a scheduled or waiting run to start.")
    cmd.add_argument(
        "run_id", metavar="RUN-ID ...", nargs="+")

    #--- test commands -----------------------------------------------

    def cmd_test0(client, args):
        time = now()
        past = args.past
        rng = args.fut - past

        for i in range(args.num):
            run = client.schedule("reruntest", {"id": i}, time + past + rng * random.random())
            log.info(f"scheduled: {run['run_id']}")

    cmd = parser.add_command(
        "test0", cmd_test0, 
        description="[TEST] Schedule reruntest.")
    cmd.add_argument("num", metavar="NUM", type=int, nargs="?", default=1)
    cmd.add_argument("fut", metavar="SECS", type=float, nargs="?", default=60)
    cmd.add_argument("past", metavar="SECS", type=float, nargs="?", default=0)

    #-------------------------------------------------------------

    args = parser.parse_args()
    client = apsis.service.client.Client((args.host, args.port))
    con = apsis.cmdline.get_console()

    try:
        args.cmd(client, args)
    except apsis.service.client.APIError as err:
        apsis.cmdline.print_api_error(err, con)
        raise SystemExit(1)
    except (KeyboardInterrupt, BrokenPipeError):
        pass


#-------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

