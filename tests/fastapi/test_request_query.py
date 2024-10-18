from http import HTTPStatus
from typing import Annotated
from typing import List
from typing import Literal
from typing import Optional

import pytest
from fastapi import Query
from fastapi.testclient import TestClient
from pydantic import field_validator
from pydantic import ValidationError

from clean_python import ComparisonFilter
from clean_python import Filter
from clean_python import InMemoryGateway
from clean_python import PageOptions
from clean_python.fastapi import get
from clean_python.fastapi import RequestQuery
from clean_python.fastapi import Resource
from clean_python.fastapi import Service
from clean_python.fastapi import v


class SomeQuery(RequestQuery):
    foo: Optional[int] = None

    @field_validator("foo", mode="after")
    @classmethod
    def validate_foo(cls, v):
        assert v is None or v < 10, "too much!"
        return v


class SomeListQuery(RequestQuery):
    foo: Optional[List[int]]


@pytest.mark.parametrize(
    "query,expected",
    [
        (
            RequestQuery(),
            PageOptions(limit=50, offset=0, order_by="id", ascending=True),
        ),
        (
            RequestQuery(limit=10, offset=20, order_by="-id"),
            PageOptions(limit=10, offset=20, order_by="id", ascending=False),
        ),
    ],
)
def test_as_page_options(query, expected):
    assert query.as_page_options() == expected


@pytest.mark.parametrize(
    "query,expected",
    [
        (SomeQuery(), []),
        (SomeQuery(foo=None), []),
        (SomeQuery(foo=3), [Filter(field="foo", values=[3])]),
        (SomeListQuery(foo=[3, 4]), [Filter(field="foo", values=[3, 4])]),
    ],
)
def test_filters(query, expected):
    assert query.filters() == expected


def test_validate_order_by():
    with pytest.raises(ValidationError):
        RequestQuery(limit=10, offset=0, order_by="foo")


class ComparisonQuery(RequestQuery):
    x__gt: int | None = None
    x__y__lt: int | None = None
    x__ge: int | None = None
    x__le: int | None = None
    x__ne: int | None = None
    x__eq: int | None = None


@pytest.mark.parametrize(
    "values,expected",
    [
        ({"x__gt": 15}, ComparisonFilter(field="x", values=[15], operator="gt")),
        ({"x__y__lt": 2}, ComparisonFilter(field="x__y", values=[2], operator="lt")),
    ],
)
def test_filters_comparison(values, expected):
    assert ComparisonQuery(**values).filters() == [expected]


class FooResource(Resource, version=v(1), name="testing"):
    @get("/query")
    
    def query(self, q: Annotated[SomeQuery, Query()]):
        return q.model_dump()


@pytest.fixture
def app():
    return Service(FooResource()).create_app(
        title="test",
        description="testing",
        hostname="testserver",
        access_logger_gateway=InMemoryGateway([]),
    )


@pytest.fixture
def client(app):
    return TestClient(app)


def test_request_query_order_by(client: TestClient):
    response = client.get("v1/query", params={"order_by": "-id"})

    assert response.status_code == HTTPStatus.OK, response.json()

    assert response.json()["order_by"] == "-id"


def test_request_query_order_by_err(client: TestClient):
    # order_by is actually caught by fastapi; it doesn't need our workaround
    response = client.get("v1/query", params={"order_by": "foo"})

    assert response.status_code == HTTPStatus.BAD_REQUEST
    body = response.json()
    assert body["message"] == "Validation error"
    (detail,) = body["detail"]
    assert detail["loc"] == ["query", "order_by"]


def test_request_query_foo_err(client: TestClient):
    # custom validator is not caught by fastapi and requires our workaround
    response = client.get("v1/query", params={"foo": 11})

    assert response.status_code == HTTPStatus.BAD_REQUEST
    body = response.json()
    assert body["message"] == "Validation error"
    (detail,) = body["detail"]
    assert detail["loc"] == ["query", "foo"]


def test_request_query_order_by_schema(client: TestClient):
    # the Literal value is correctly reflected as an enum in the openapi spec
    openapi = client.get("v1/openapi.json", params={"order_by": "foo"}).json()

    parameters = {x["name"]: x for x in openapi["paths"]["/query"]["get"]["parameters"]}
    assert parameters["order_by"]["schema"]["enum"] == ["id", "-id"]


def test_request_query_order_by_deprecated_enum_arg():
    with pytest.raises(ValueError):

        class EnumQuery(RequestQuery):
            order_by: str = Query(default="id", enum=["id", "-id"])


def test_request_query_order_by_correct_enum_arg():
    class EnumQuery(RequestQuery):
        order_by: Literal["id", "-id"] = Query(default="id")
