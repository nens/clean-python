import pytest

from clean_python import ComparisonFilter
from clean_python import ComparisonOperator
from clean_python import Filter


def test_filter_for_id():
    actual = Filter.for_id(2)
    assert actual.field == "id"
    assert actual.values == [2]


def test_comparison_filter_init():
    actual = ComparisonFilter(field="foo", values=[2], operator="gt")
    assert actual.operator is ComparisonOperator.GT


@pytest.mark.parametrize("values", [[], [1, 2]])
def test_comparison_filter_err(values):
    with pytest.raises(ValueError):
        ComparisonFilter(field="foo", values=values, operator="gt")
