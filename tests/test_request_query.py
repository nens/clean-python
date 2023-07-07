from typing import Optional

import pytest
from pydantic import ValidationError

from clean_python.base.domain.pagination import PageOptions
from clean_python.base.infrastructure.gateway import Filter
from clean_python.fastapi.request_query import RequestQuery


class SomeQuery(RequestQuery):
    foo: Optional[int] = None


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
    ],
)
def test_filters(query, expected):
    assert query.filters() == expected


def test_validate_order_by():
    with pytest.raises(ValidationError):
        RequestQuery(limit=10, offset=0, order_by="foo")
