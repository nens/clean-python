from pathlib import Path

from celery import bootsteps
from celery.signals import worker_ready
from celery.signals import worker_shutdown

__all__ = ["setup_kubernetes_probes"]


HEARTBEAT_FILE = Path("/dev/shm/worker_heartbeat")
READINESS_FILE = Path("/dev/shm/worker_ready")


def register_readiness(**_):
    READINESS_FILE.touch()


def unregister_readiness(**_):
    READINESS_FILE.unlink(missing_ok=True)


class LivenessProbe(bootsteps.StartStopStep):
    requires = {"celery.worker.components:Timer"}

    def __init__(self, worker, **kwargs):
        self.requests = []
        self.tref = None

    def start(self, worker):
        self.tref = worker.timer.call_repeatedly(
            1.0,
            self.update_heartbeat_file,
            (worker,),
            priority=10,
        )

    def stop(self, worker):
        HEARTBEAT_FILE.unlink(missing_ok=True)

    def update_heartbeat_file(self, worker):
        HEARTBEAT_FILE.touch()


def setup_kubernetes_probes(app):
    worker_ready.connect(register_readiness)
    worker_shutdown.connect(unregister_readiness)

    app.steps["worker"].add(LivenessProbe)
