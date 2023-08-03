import os

import debugpy

__all__ = ["setup_debugger"]


def setup_debugger(*, host: str = "0.0.0.0", port: int = 5678):
    """Configure debugging via debugpy."""
    debugpy.listen((host, port))
    if os.environ.get("DEBUG_WAIT_FOR_CLIENT"):
        print("🔌 debugpy waiting for a client to attach 🔌", flush=True)
        debugpy.wait_for_client()
