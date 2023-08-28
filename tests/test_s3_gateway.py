from typing import Callable
from unittest import mock

import pytest

from clean_python.s3 import S3Gateway


@pytest.fixture
def mocked_s3_provider():
    return mock.MagicMock()


@pytest.fixture
def s3_gateway(mocked_s3_provider):
    return S3Gateway(mocked_s3_provider)


def no_page():
    yield {}


def one_page():
    yield {"Contents": [{"Size": 1}, {"Size": 2}, {"Size": 3}]}


def two_pages():
    yield {"Contents": [{"Size": 1}, {"Size": 2}, {"Size": 3}]}
    yield {"Contents": [{"Size": 4}]}


@pytest.mark.parametrize("f, expected", [(no_page, 0), (one_page, 6), (two_pages, 10)])
async def test_get_size(
    f: Callable[[], dict], expected: int, mocked_s3_provider, s3_gateway: S3Gateway
):
    pages = mock.MagicMock()
    pages.__aiter__.return_value = f()
    paginator = mock.Mock()
    paginator.paginate = mock.Mock(return_value=pages)
    mocked_s3_provider.get_client.return_value.__aenter__.return_value.get_paginator = (
        mock.Mock(return_value=paginator)
    )

    actual = await s3_gateway.get_size("raster-1/")
    assert actual == expected
