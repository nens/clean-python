from typing import List
from typing import Optional

import pytest
from pydantic import ValidationError

from clean_python import ComparisonFilter
from clean_python import Filter
from clean_python import PageOptions
from clean_python.fastapi import RequestQuery


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
