from uuid import UUID
from uuid import uuid4

from starlette.requests import Request

from clean_python import Tenant

CORRELATION_ID_HEADER = b"x-correlation-id"
TENANT_ID_HEADER = b"x-tenant-id"


def get_view_name(request: Request) -> str | None:
    try:
        view_name = request.scope["route"].name
    except KeyError:
        return None

    return view_name


def is_health_check(request: Request) -> bool:
    return get_view_name(request) == "health_check"


def get_header(request: Request, header_name: bytes) -> str | None:
    headers = dict(request.scope["headers"])
    try:
        return headers[header_name].decode()
    except (KeyError, ValueError, UnicodeDecodeError):
        return None


def get_tenant(request: Request) -> Tenant | None:
    tenant_id = get_header(request, TENANT_ID_HEADER)
    return None if tenant_id is None else Tenant(id=tenant_id, name="")


def get_correlation_id(request: Request) -> UUID | None:
    header = get_header(request, CORRELATION_ID_HEADER)
    if header is None:
        return None
    try:
        return UUID(header)
    except ValueError:
        return None


def ensure_correlation_id(request: Request) -> None:
    correlation_id = get_correlation_id(request)
    if correlation_id is None:
        # generate an id and update the request inplace
        correlation_id = uuid4()
        headers = dict(request.scope["headers"])
        headers[CORRELATION_ID_HEADER] = str(correlation_id).encode()
        request.scope["headers"] = list(headers.items())
