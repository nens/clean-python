import pytest
from pydantic import field_validator
from pydantic import ValidationError

from clean_python import BadRequest
from clean_python import ValueObject


class Color(ValueObject):
    name: str

    @field_validator("name")
    def name_not_empty(cls, v, _):
        assert v != ""
        return v


@pytest.fixture
def color():
    return Color(name="green")


def test_validator():
    with pytest.raises(ValidationError) as e:
        Color(name="")

    assert e.type is ValidationError  # not BadRequest


def test_create_err():
    with pytest.raises(BadRequest):
        Color.create(name="")


def test_update(color):
    updated = color.update(name="red")

    assert color.name == "green"
    assert updated.name == "red"


def test_update_validates(color):
    with pytest.raises(BadRequest):
        color.update(name="")


def test_run_validation(color):
    assert color.run_validation() == color


def test_run_validation_err():
    color = Color.model_construct(name="")

    with pytest.raises(BadRequest):
        color.run_validation()


def test_hashable(color):
    assert len({color, color}) == 1


def test_eq(color):
    assert color == color


def test_neq(color):
    assert color != Color(name="red")
