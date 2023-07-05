from typing import Optional

import pytest
from pydantic import ValidationError

from base_lib.gateway import Filter
from base_lib.pagination import PageOptions
from base_lib.request_query import RequestQuery


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
