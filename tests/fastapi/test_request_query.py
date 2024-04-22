from http import HTTPStatus
from typing import List
from typing import Optional

import pytest
from fastapi.testclient import TestClient
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
    def query(self, q: SomeQuery = SomeQuery.depends()):
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
    response = client.get("v1/query", params={"order_by": "foo"})

    assert response.status_code == HTTPStatus.BAD_REQUEST
    body = response.json()
    assert body["message"] == "Validation error"
    (detail,) = body["detail"]
    assert detail["loc"] == ["order_by"]


def test_request_query_order_by_schema(client: TestClient):
    openapi = client.get("v1/openapi.json", params={"order_by": "foo"}).json()

    parameters = {x["name"]: x for x in openapi["paths"]["/query"]["get"]["parameters"]}
    assert len(parameters) == 4
    assert parameters["limit"] == {
        "name": "limit",
        "in": "query",
        "required": False,
        "schema": {
            "type": "integer",
            "maximum": 100,
            "minimum": 1,
            "default": 50,
            "title": "Limit",
        },
    }
    assert parameters["offset"] == {
        "name": "offset",
        "in": "query",
        "required": False,
        "schema": {"type": "integer", "minimum": 0, "default": 0, "title": "Offset"},
    }
    assert parameters["order_by"] == {
        "name": "order_by",
        "in": "query",
        "required": False,
        "schema": {"type": "string", "default": "id", "title": "Order By"},
    }
    assert parameters["foo"] == {
        "name": "foo",
        "in": "query",
        "required": False,
        "schema": {"anyOf": [{"type": "integer"}, {"type": "null"}], "title": "Foo"},
    }
