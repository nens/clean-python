from locust import HttpUser
from locust import tag
from locust import task


class LoadUser(HttpUser):
    host = "http://localhost:8000/v1"

    @tag("sleep")
    @task
    def sleep(self):
        self.client.get("/sleep/20")

    @tag("get")
    @task
    def get(self):
        self.client.get("/get/1")
