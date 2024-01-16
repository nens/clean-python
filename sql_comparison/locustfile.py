import os

from locust import HttpUser
from locust import task

HOST = "http://localhost:8000"  # "https://raster-loadtest.lizard.net"
BACKEND = os.environ.get("SQL_COMPARISON_BACKEND", "sqlalchemy")
QUERY_DURATION = int(os.environ.get("SQL_COMPARISON_QUERY_DURATION", 10))

# asyncpg: max 216 rps (for 30 users), p50 110 ms
# sqlalchemy: max 55 rps (for 16 users), p50 210 ms
# sqlalchemy_sync: max 63 rps (for 18 users), p50 210 ms
# sqlalchmemy_nullpool: max 14 rps (for 9 users), p50 914 ms


class LoadUser(HttpUser):
    host = HOST

    @task
    def wms(self):
        self.client.get(f"/v1/{BACKEND}/{QUERY_DURATION}")
