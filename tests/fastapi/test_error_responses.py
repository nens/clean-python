import json
import logging
from http import HTTPStatus

from pydantic import BaseModel
from pydantic import ValidationError

from clean_python import BadRequest
from clean_python import Conflict
from clean_python import DoesNotExist
from clean_python import PermissionDenied
from clean_python import Unauthorized
from clean_python.fastapi.error_responses import conflict_handler
from clean_python.fastapi.error_responses import not_found_handler
from clean_python.fastapi.error_responses import permission_denied_handler
from clean_python.fastapi.error_responses import unauthorized_handler
from clean_python.fastapi.error_responses import validation_error_handler


async def test_does_not_exist():
    actual = await not_found_handler(None, DoesNotExist("record", id=15))

    assert actual.status_code == HTTPStatus.NOT_FOUND
    assert json.loads(actual.body) == {"message": "Could not find record with id=15"}


async def test_does_not_exist_no_id():
    actual = await not_found_handler(None, DoesNotExist("tafeltje"))

    assert actual.status_code == HTTPStatus.NOT_FOUND
    assert json.loads(actual.body) == {"message": "Could not find tafeltje"}


async def test_conflict():
    actual = await conflict_handler(None, Conflict("foo"))

    assert actual.status_code == HTTPStatus.CONFLICT
    assert json.loads(actual.body) == {"message": "Conflict", "detail": "foo"}


async def test_conflict_no_msg():
    actual = await conflict_handler(None, Conflict())

    assert actual.status_code == HTTPStatus.CONFLICT
    assert json.loads(actual.body) == {"message": "Conflict", "detail": None}


async def test_unauthorized(caplog):
    actual = await unauthorized_handler(None, Unauthorized())

    assert actual.status_code == HTTPStatus.UNAUTHORIZED
    assert json.loads(actual.body) == {"message": "Unauthorized", "detail": None}
    assert actual.headers["WWW-Authenticate"] == "Bearer"

    assert caplog.record_tuples == []


async def test_unauthorized_wit_msg(caplog):
    caplog.set_level(logging.INFO)

    # message should be ignored
    actual = await unauthorized_handler(None, Unauthorized("foo"))

    assert actual.status_code == HTTPStatus.UNAUTHORIZED
    assert json.loads(actual.body) == {"message": "Unauthorized", "detail": None}
    assert actual.headers["WWW-Authenticate"] == "Bearer"

    assert caplog.record_tuples == [
        ("clean_python.fastapi.error_responses", logging.INFO, "unauthorized: foo")
    ]


async def test_permission_denied():
    actual = await permission_denied_handler(None, PermissionDenied())

    assert actual.status_code == HTTPStatus.FORBIDDEN
    assert json.loads(actual.body) == {"message": "Permission denied", "detail": None}


async def test_permission_denied_with_msg():
    actual = await permission_denied_handler(None, PermissionDenied("foo"))

    assert actual.status_code == HTTPStatus.FORBIDDEN
    assert json.loads(actual.body) == {"message": "Permission denied", "detail": "foo"}


class Book(BaseModel):
    title: str


async def test_validation_error():
    try:
        Book(name="foo")
    except ValidationError as e:
        actual = await validation_error_handler(None, e)

    assert actual.status_code == HTTPStatus.BAD_REQUEST
    assert json.loads(actual.body) == {
        "message": "Validation error",
        "detail": [{"loc": ["title"], "msg": "Field required", "type": "missing"}],
    }


async def test_bad_request_from_validation_error():
    try:
        Book(name="foo")
    except ValidationError as e:
        actual = await validation_error_handler(None, BadRequest(e))

    assert actual.status_code == HTTPStatus.BAD_REQUEST
    assert json.loads(actual.body) == {
        "message": "Validation error",
        "detail": [{"loc": ["title"], "msg": "Field required", "type": "missing"}],
    }


async def test_bad_request_from_msg():
    actual = await validation_error_handler(None, BadRequest("foo"))

    assert actual.status_code == HTTPStatus.BAD_REQUEST
    assert json.loads(actual.body) == {
        "message": "Validation error",
        "detail": [{"loc": [], "msg": "foo", "type": "value_error"}],
    }
