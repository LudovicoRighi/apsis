import logging
import os
from   procstar.agent.testing import get_procstar_path, TLS_CERT_PATH
import secrets
import signal
import subprocess
import uuid

import instance

logger = logging.getLogger(__name__)

#-------------------------------------------------------------------------------

# Default agent port for testing, distinct from the usual default.
DEFAULT_AGENT_PORT = 59790

# Environment containing auth info for testing.
AUTH_ENV = {
    "PROCSTAR_AGENT_CERT": TLS_CERT_PATH,
    "PROCSTAR_AGENT_TOKEN": secrets.token_urlsafe(32),
}

class Agent:
    """
    A Procstar agent process.
    """

    def __init__(
            self, *,
            host,
            port,
            conn_id     =None,
            group_id    ="default",
    ):
        if conn_id is None:
            conn_id = str(uuid.uuid4())

        self.group_id   = group_id
        self.conn_id    = conn_id
        self.host       = host
        self.port       = port

        self.proc       = None


    def start(self):
        assert self.proc is None, "already started"

        argv = [
            get_procstar_path(),
            "--log-level", "trace",
            "--agent",
            "--agent-host", self.host,
            "--agent-port", str(self.port),
            "--group-id", self.group_id,
            "--conn-id", self.conn_id,
            "--connect-interval-start", "0.1",
            "--connect-interval-max", "0.1",
        ]
        env = os.environ | AUTH_ENV | {
            "RUST_BACKTRACE": "1",
        }
        self.proc = subprocess.Popen(argv, env=env)
        logger.info(f"started Procstar agent at pid {self.proc.pid}")


    def close(self):
        if self.proc is not None:
            logging.info("killing Procstar agent")
            self.proc.send_signal(signal.SIGKILL)
            self.proc.wait()
            self.proc = None


    def __enter__(self):
        self.start()
        return self


    def __exit__(self, *exc_info):
        self.close()



class ApsisService(instance.ApsisService):

    def __init__(self, *, cfg={}, **kw_args):
        self.agent_port = DEFAULT_AGENT_PORT

        # FIXME: Choose an available port.
        cfg |= {
            "procstar": {
                "agent": {
                    "enable": True,
                    "server": {
                        "port": self.agent_port,
                    },
                    "connection": {
                        "start_timeout": 2,
                        "reconnect_timeout": 2,
                    },
                },
            },
        }
        super().__init__(cfg=cfg, env=AUTH_ENV, **kw_args)


    def agent(self, **kw_args):
        return Agent(host="localhost", port=self.agent_port, **kw_args)



