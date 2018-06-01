import asyncio
import getpass
import jinja2
import logging
from   pathlib import Path
import shlex
import socket

from   .types import ProgramError, ProgramFailure

log = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

# FIXME: Elsewhere.

def template_expand(template, args):
    return jinja2.Template(template).render(args)


def join_args(argv):
    return " ".join( shlex.quote(a) for a in argv )


#-------------------------------------------------------------------------------

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%kZ"

class ProcessProgram:

    def __init__(self, argv):
        self.__argv = tuple( str(a) for a in argv )


    def __str__(self):
        return join_args(self.__argv)


    def bind(self, args):
        argv = tuple( template_expand(a, args) for a in self.__argv )
        return type(self)(argv)


    def to_jso(self):
        return {
            "argv"      : list(self.__argv),
        }


    async def start(self, run):
        # FIXME: Start / end time one level up.
        run.meta.update({
            "hostname"  : socket.gethostname(),
            "username"  : getpass.getuser(),
        })

        argv = self.__argv
        log.info("starting: {}".format(join_args(argv)))

        try:
            with open("/dev/null") as stdin:
                proc = await asyncio.create_subprocess_exec(
                    *argv, 
                    executable  =Path(argv[0]),
                    stdin       =stdin,
                    # Merge stderr with stdin.  FIXME: Do better.
                    stdout      =asyncio.subprocess.PIPE,
                    stderr      =asyncio.subprocess.STDOUT,
                )
        except OSError as exc:
            raise ProgramError(str(exc))
        else:
            run.meta["pid"] = proc.pid
            return proc


    async def wait(self, run, proc):
        stdout, stderr  = await proc.communicate()
        return_code     = proc.returncode

        assert stderr is None
        assert return_code is not None

        run.meta["return_code"] = return_code
        run.output = stdout
        log.info(f"complete with return code {return_code}")
        if return_code == 0:
            return
        else:
            raise ProgramFailure("return code = {}".format(return_code))



class ShellCommandProgram(ProcessProgram):

    def __init__(self, command):
        # FIXME: Which shell?
        command = str(command)
        super().__init__(["/bin/bash", "-c", command])
        self.__command = command


    def bind(self, args):
        command = template_expand(self.__command, args)
        return type(self)(command)


    def __str__(self):
        return self.__command



