import os


def setup_debugger(*, host: str = "0.0.0.0", port: int = 5678):
    """Configure debugging via debugpy."""

    # Only to be used in development. Should someone inadvertently set DEBUG to True in
    # staging or production, a ModuleNotFoundError will be raised, because debugpy is
    # only available via requirements-dev.txt - this is intentionally.
    if os.environ.get("DEBUG") or os.environ.get("DEBUG_WAIT_FOR_CLIENT"):
        try:
            import debugpy

            debugpy.listen((host, port))
            if os.environ.get("DEBUG_WAIT_FOR_CLIENT"):
                print("🔌 debugpy waiting for a client to attach 🔌", flush=True)
                debugpy.wait_for_client()
        except (ModuleNotFoundError, RuntimeError) as e:
            print(e, flush=True)
