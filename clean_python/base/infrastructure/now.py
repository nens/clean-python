from datetime import datetime
from datetime import timezone


def now():
    # this function is there so that we can mock it in tests
    return datetime.now(timezone.utc)
