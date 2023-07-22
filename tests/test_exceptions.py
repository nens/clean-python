from pydantic import ValidationError

from clean_python import BadRequest
from clean_python import DoesNotExist
from clean_python import ValueObject


def test_bad_request_short_str():
    e = BadRequest("bla bla bla")
    assert str(e) == "bla bla bla"


def test_does_not_exist_str():
    e = DoesNotExist("raster", id=12)
    assert str(e) == "does not exist: raster with id=12"


def test_does_not_exist_no_id_str():
    e = DoesNotExist("raster")
    assert str(e) == "does not exist: raster"


class Book(ValueObject):
    title: str


def test_bad_request_from_validation_error():
    try:
        Book()
    except ValidationError as e:
        err = BadRequest(e)

    assert str(err) == "validation error: 'title' Field required"
