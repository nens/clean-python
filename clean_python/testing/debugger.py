import debugpy

__all__ = ["setup_debugger"]


def setup_debugger(
    *, host: str = "0.0.0.0", port: int = 5678, wait_for_client: bool = False
):
    """Configure debugging via debugpy."""
    debugpy.listen((host, port))
    if wait_for_client:
        print("🔌 debugpy waiting for a client to attach 🔌", flush=True)
        debugpy.wait_for_client()
