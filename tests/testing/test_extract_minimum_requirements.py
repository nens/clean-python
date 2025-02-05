from unittest.mock import Mock
from unittest.mock import patch

import pytest
from packaging.requirements import Requirement
from packaging.specifiers import Specifier
from packaging.specifiers import SpecifierSet

from clean_python.testing.extract_minimum_requirements import get_minimum_requirements
from clean_python.testing.extract_minimum_requirements import get_requirements
from clean_python.testing.extract_minimum_requirements import specifier_set_to_minimum
from clean_python.testing.extract_minimum_requirements import specifier_to_minimum


@pytest.mark.parametrize(
    "input,expected",
    [(">=1.0.0", "==1.0.0"), ("<1.0.0", "<1.0.0"), ("==1.0.0", "==1.0.0")],
)
def test_specifier_to_minimum(input: str, expected: str):
    assert str(specifier_to_minimum(Specifier(input))) == expected


def test_specifier_set_to_minimum():
    assert str(specifier_set_to_minimum(SpecifierSet(">=1.0,<2"))) == SpecifierSet(
        "==1.0,<2"
    )


@patch("clean_python.testing.extract_minimum_requirements.requires")
def test_get_requirements(mock_requires: Mock):
    mock_requires.return_value = ["foo==2.9", "bar>=5"]
    assert get_requirements("mypackage", {}) == [
        Requirement("foo==2.9"),
        Requirement("bar>=5"),
    ]
    mock_requires.assert_called_once_with("mypackage")


@patch("clean_python.testing.extract_minimum_requirements.requires")
def test_get_requirements_with_extra(mock_requires: Mock):
    mock_requires.return_value = ['foo==2.9; extra == "a"', 'bar>=5; extra == "b"']
    assert get_requirements("fastapi", {"a"}) == [Requirement("foo==2.9; extra == 'a'")]


@patch("clean_python.testing.extract_minimum_requirements.requires")
def test_get_requirements_minimum_requirements_integration(mock_requires: Mock):
    mock_requires.return_value = ["foo>=2.9", 'bar>=5; extra == "b"']
    assert get_minimum_requirements("fastapi", {"b"}) == [
        Requirement("foo==2.9"),
        Requirement("bar==5"),
    ]
