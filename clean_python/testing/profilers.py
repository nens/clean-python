# (c) Nelen & Schuurmans

from pathlib import Path

import dramatiq
import yappi

__all__ = ["ProfilerMiddleware"]

PROFILE_DIR = "var"


class ProfilerMiddleware(dramatiq.Middleware):
    """For usage with dramatiq (single-threaded only)"""

    def __init__(self, profile_dir: Path):
        profile_dir.mkdir(exist_ok=True)
        self.profile_dir = profile_dir

    def before_process_message(self, broker, message):
        yappi.set_clock_type("wall")
        yappi.start()

    def after_process_message(
        self, broker, message: dramatiq.Message, *, result=None, exception=None
    ):
        yappi.stop()

        stats = yappi.convert2pstats(yappi.get_func_stats())

        stats.dump_stats(
            self.profile_dir / f"{message.actor_name}-{message.message_id}.pstats"
        )

        yappi.clear_stats()
