import io
from unittest import mock

import pytest
from urllib3.response import HTTPResponse
from urllib3.util.request import set_file_position

from clean_python.api_client import ApiException
from clean_python.api_client import download_file
from clean_python.api_client import download_fileobj
from clean_python.api_client import upload_file
from clean_python.api_client import upload_fileobj
from clean_python.api_client.files import _SeekableChunkIterator
from clean_python.api_client.files import DEFAULT_UPLOAD_TIMEOUT

MODULE = "clean_python.api_client.files"


@pytest.fixture
def pool():
    pool = mock.Mock()
    return pool


@pytest.fixture
def responses_single():
    return [
        HTTPResponse(
            body=b"X" * 42,
            headers={"Content-Range": "bytes 0-41/42"},
            status=206,
        )
    ]


@pytest.fixture
def responses_double():
    return [
        HTTPResponse(
            body=b"X" * 64,
            headers={"Content-Range": "bytes 0-63/65"},
            status=206,
        ),
        HTTPResponse(
            body=b"X",
            headers={"Content-Range": "bytes 64-64/65"},
            status=206,
        ),
    ]


def test_download_fileobj(pool, responses_single):
    stream = io.BytesIO()
    pool.request.side_effect = responses_single
    download_fileobj("some-url", stream, chunk_size=64, pool=pool)

    pool.request.assert_called_with(
        "GET",
        "some-url",
        headers={"Range": "bytes=0-63"},
        timeout=5.0,
    )
    assert stream.tell() == 42


def test_download_fileobj_two_chunks(pool, responses_double):
    stream = io.BytesIO()
    pool.request.side_effect = responses_double

    callback_func = mock.Mock()

    download_fileobj(
        "some-url", stream, chunk_size=64, pool=pool, callback_func=callback_func
    )

    (_, kwargs1), (_, kwargs2) = pool.request.call_args_list
    assert kwargs1["headers"] == {"Range": "bytes=0-63"}
    assert kwargs2["headers"] == {"Range": "bytes=64-127"}
    assert stream.tell() == 65

    # Check callback func
    (args1, _), (args2, _) = callback_func.call_args_list

    assert args1 == (63, 65)
    assert args2 == (65, 65)


def test_download_fileobj_no_multipart(pool, responses_single):
    """The remote server does not support range requests"""
    responses_single[0].status = 200
    pool.request.side_effect = responses_single

    with pytest.raises(ApiException) as e:
        download_fileobj("some-url", None, chunk_size=64, pool=pool)

    assert e.value.status == 200
    assert str(e.value) == "200: The file server does not support multipart downloads."


def test_download_fileobj_forbidden(pool, responses_single):
    """The remote server does not support range requests"""
    responses_single[0].status = 403
    pool.request.side_effect = responses_single

    with pytest.raises(ApiException) as e:
        download_fileobj("some-url", None, chunk_size=64, pool=pool)

    assert e.value.status == 403


@mock.patch(MODULE + ".download_fileobj")
def test_download_file(download_fileobj, tmp_path):
    download_file(
        "http://domain/a.b",
        tmp_path / "c.d",
        chunk_size=64,
        timeout=3.0,
        pool="foo",
        headers_factory="bar",
    )

    args, kwargs = download_fileobj.call_args
    assert args[0] == "http://domain/a.b"
    assert isinstance(args[1], io.IOBase)
    assert args[1].mode == "wb"
    assert args[1].name == str(tmp_path / "c.d")
    assert kwargs["chunk_size"] == 64
    assert kwargs["timeout"] == 3.0
    assert kwargs["pool"] == "foo"
    assert kwargs["headers_factory"] == "bar"


@mock.patch(MODULE + ".download_fileobj")
def test_download_file_directory(download_fileobj, tmp_path):
    # if a target directory is specified, a filename is created from the url
    download_file("http://domain/a.b", tmp_path, chunk_size=64, timeout=3.0, pool="foo")

    args, kwargs = download_fileobj.call_args
    assert args[1].name == str(tmp_path / "a.b")


@pytest.fixture
def upload_response():
    return HTTPResponse(status=200)


@pytest.fixture
def fileobj():
    stream = io.BytesIO()
    stream.write(b"X" * 39)
    stream.seek(0)
    return stream


@pytest.mark.parametrize(
    "chunk_size,expected_body",
    [
        (64, [b"X" * 39]),
        (39, [b"X" * 39]),
        (38, [b"X" * 38, b"X"]),
        (16, [b"X" * 16, b"X" * 16, b"X" * 7]),
    ],
)
def test_upload_fileobj(pool, fileobj, upload_response, chunk_size, expected_body):
    pool.request.return_value = upload_response
    upload_fileobj("some-url", fileobj, chunk_size=chunk_size, pool=pool)

    args, kwargs = pool.request.call_args
    assert args == ("PUT", "some-url")
    assert list(kwargs["body"]) == expected_body
    assert kwargs["headers"] == {"Content-Length": "39"}
    assert kwargs["timeout"] == DEFAULT_UPLOAD_TIMEOUT


def test_upload_fileobj_callback(pool, fileobj, upload_response):
    expected_body = [b"X" * 16, b"X" * 16, b"X" * 7]
    chunk_size = 16

    pool.request.return_value = upload_response
    callback_func = mock.Mock()

    upload_fileobj(
        "some-url",
        fileobj,
        chunk_size=chunk_size,
        pool=pool,
        callback_func=callback_func,
    )

    args, kwargs = pool.request.call_args
    assert args == ("PUT", "some-url")
    assert list(kwargs["body"]) == expected_body
    assert kwargs["headers"] == {"Content-Length": "39"}
    assert kwargs["timeout"] == DEFAULT_UPLOAD_TIMEOUT

    # Check callback_func
    (args1, _), (args2, _), (args3, _) = callback_func.call_args_list
    assert args1 == (16, 39)
    assert args2 == (32, 39)
    assert args3 == (39, 39)


def test_upload_fileobj_with_md5(pool, fileobj, upload_response):
    pool.request.return_value = upload_response
    upload_fileobj("some-url", fileobj, pool=pool, md5=b"abcd")

    # base64.b64encode(b"abcd")).decode()
    expected_md5 = "YWJjZA=="

    args, kwargs = pool.request.call_args
    assert kwargs["headers"] == {"Content-Length": "39", "Content-MD5": expected_md5}


def test_upload_fileobj_empty_file():
    with pytest.raises(IOError, match="The file object is empty."):
        upload_fileobj("some-url", io.BytesIO())


def test_upload_fileobj_non_binary_file():
    with pytest.raises(IOError, match="The file object is not in binary*"):
        upload_fileobj("some-url", io.StringIO())


def test_upload_fileobj_errors(pool, fileobj, upload_response):
    upload_response.status = 400
    pool.request.return_value = upload_response
    with pytest.raises(ApiException) as e:
        upload_fileobj("some-url", fileobj, pool=pool)

    assert e.value.status == 400


@mock.patch(MODULE + ".upload_fileobj")
def test_upload_file(upload_fileobj, tmp_path):
    path = tmp_path / "myfile"
    with path.open("wb") as f:
        f.write(b"X")

    upload_file(
        "http://domain/a.b",
        path,
        chunk_size=1234,
        timeout=3.0,
        pool="foo",
        md5=b"abcd",
        headers_factory="bar",
    )

    args, kwargs = upload_fileobj.call_args
    assert args[0] == "http://domain/a.b"
    assert isinstance(args[1], io.IOBase)
    assert args[1].mode == "rb"
    assert args[1].name == str(path)
    assert kwargs["timeout"] == 3.0
    assert kwargs["chunk_size"] == 1234
    assert kwargs["pool"] == "foo"
    assert kwargs["md5"] == b"abcd"
    assert kwargs["headers_factory"] == "bar"


def test_seekable_chunk_iterator():
    data = b"XYZ"
    body = _SeekableChunkIterator(io.BytesIO(data), chunk_size=4)

    pos = set_file_position(body, pos=0)
    assert pos == 0
    assert list(body) == [data]
    assert list(body) == []
    set_file_position(body, pos)
    assert list(body) == [data]


def test_download_fileobj_with_headers(pool, responses_single):
    pool.request.side_effect = responses_single
    download_fileobj(
        "some-url",
        io.BytesIO(),
        chunk_size=64,
        pool=pool,
        headers_factory=lambda: {"foo": "bar"},
    )

    pool.request.assert_called_with(
        "GET",
        "some-url",
        headers={"Range": "bytes=0-63", "foo": "bar"},
        timeout=5.0,
    )


def test_upload_fileobj_with_headers(pool, fileobj, upload_response):
    pool.request.return_value = upload_response
    upload_fileobj(
        "some-url", fileobj, pool=pool, headers_factory=lambda: {"foo": "bar"}
    )

    _, kwargs = pool.request.call_args
    assert kwargs["headers"] == {"Content-Length": "39", "foo": "bar"}
