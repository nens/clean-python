from clean_python import InMemoryGateway
from clean_python.fastapi import Service

from .presentation import V1Books

service = Service(V1Books())

app = service.create_app(
    title="Book service",
    description="Service for testing clean-python",
    hostname="testserver",
    access_logger_gateway=InMemoryGateway([]),
)
