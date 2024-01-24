from locust import HttpUser
from locust import tag
from locust import task


class LoadUser(HttpUser):
    host = "http://localhost:8000/v1"

    @tag("gateway")
    @task
    def gateway(self):
        self.client.get("/gateway/1")
