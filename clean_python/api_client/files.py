import base64
import hashlib
import logging
import os
import re
from http import HTTPStatus
from pathlib import Path
from typing import BinaryIO
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union
from urllib.parse import urlparse

import urllib3

from .exceptions import ApiException

__all__ = ["download_file", "download_fileobj", "upload_file", "upload_fileobj"]


CONTENT_RANGE_REGEXP = re.compile(r"^bytes (\d+)-(\d+)/(\d+|\*)$")
# Default upload timeout has an increased socket read timeout, because MinIO
# takes very long for completing the upload for larger files. The limit of 10 minutes
# should accomodate files up to 150 GB.
DEFAULT_UPLOAD_TIMEOUT = urllib3.Timeout(connect=5.0, read=600.0)


logger = logging.getLogger(__name__)


def get_pool(retries: int = 3, backoff_factor: float = 1.0) -> urllib3.PoolManager:
    """Create a PoolManager with a retry policy.

    The default retry policy has 3 retries with 1, 2, 4 second intervals.

    Args:
        retries: Total number of retries per request
        backoff_factor: Multiplier for retry delay times (1, 2, 4, ...)
    """
    return urllib3.PoolManager(
        retries=urllib3.util.retry.Retry(retries, backoff_factor=backoff_factor)
    )


def compute_md5(fileobj: BinaryIO, chunk_size: int = 16777216):
    """Compute the MD5 checksum of a file object."""
    fileobj.seek(0)
    hasher = hashlib.md5()
    for chunk in _iter_chunks(fileobj, chunk_size=chunk_size):
        hasher.update(chunk)
    return hasher.digest()


def download_file(
    url: str,
    target: Path,
    chunk_size: int = 16777216,
    timeout: Optional[Union[float, urllib3.Timeout]] = 5.0,
    pool: Optional[urllib3.PoolManager] = None,
    callback_func: Optional[Callable[[int, int], None]] = None,
    headers_factory: Optional[Callable[[], Dict[str, str]]] = None,
) -> Tuple[Path, int]:
    """Download a file to a specified path on disk.

    It is assumed that the file server supports multipart downloads (range
    requests).

    Args:
        url: The url to retrieve.
        target: The location to copy to. If this is an existing file, it is
            overwritten. If it is a directory, a filename is generated from
            the filename in the url.
        chunk_size: The number of bytes per request. Default: 16MB.
        timeout: The total timeout in seconds.
        pool: If not supplied, a default connection pool will be
            created with a retry policy of 3 retries after 1, 2, 4 seconds.
        callback_func: optional function used to receive: bytes_downloaded, total_bytes
            for example: def callback(bytes_downloaded: int, total_bytes: int) -> None
        headers_factory: optional function to inject headers

    Returns:
        Tuple of file path, total number of downloaded bytes.

    Raises:
        ApiException: raised on unexpected server
            responses (HTTP status codes other than 206, 413, 429, 503)
        urllib3.exceptions.HTTPError: various low-level HTTP errors that persist
            after retrying: connection errors, timeouts, decode errors,
            invalid HTTP headers, payload too large (HTTP 413), too many
            requests (HTTP 429), service unavailable (HTTP 503)
    """
    # cast string to Path if necessary
    if isinstance(target, str):
        target = Path(target)

    # if it is a directory, take the filename from the url
    if target.is_dir():
        target = target / urlparse(url)[2].rsplit("/", 1)[-1]

    # open the file
    try:
        with target.open("wb") as fileobj:
            size = download_fileobj(
                url,
                fileobj,
                chunk_size=chunk_size,
                timeout=timeout,
                pool=pool,
                callback_func=callback_func,
                headers_factory=headers_factory,
            )
    except Exception:
        # Clean up a partially downloaded file
        try:
            os.remove(target)
        except FileNotFoundError:
            pass
        raise

    return target, size


def download_fileobj(
    url: str,
    fileobj: BinaryIO,
    chunk_size: int = 16777216,
    timeout: Optional[Union[float, urllib3.Timeout]] = 5.0,
    pool: Optional[urllib3.PoolManager] = None,
    callback_func: Optional[Callable[[int, int], None]] = None,
    headers_factory: Optional[Callable[[], Dict[str, str]]] = None,
) -> int:
    """Download a url to a file object using multiple requests.

    It is assumed that the file server supports multipart downloads (range
    requests).

    Args:
        url: The url to retrieve.
        fileobj: The (binary) file object to write into.
        chunk_size: The number of bytes per request. Default: 16MB.
        timeout: The total timeout in seconds.
        pool: If not supplied, a default connection pool will be
            created with a retry policy of 3 retries after 1, 2, 4 seconds.
        callback_func: optional function used to receive: bytes_downloaded, total_bytes
            for example: def callback(bytes_downloaded: int, total_bytes: int) -> None
        headers_factory: optional function to inject headers

    Returns:
        The total number of downloaded bytes.

    Raises:
        ApiException: raised on unexpected server
            responses (HTTP status codes other than 206, 413, 429, 503)
        urllib3.exceptions.HTTPError: various low-level HTTP errors that persist
            after retrying: connection errors, timeouts, decode errors,
            invalid HTTP headers, payload too large (HTTP 413), too many
            requests (HTTP 429), service unavailable (HTTP 503)

        Note that the fileobj might be partially filled with data in case of
        an exception.
    """
    if pool is None:
        pool = get_pool()
    if headers_factory is not None:
        base_headers = headers_factory()
        if any(x.lower() == "range" for x in base_headers):
            raise ValueError("Cannot set the Range header through header_factory")
    else:
        base_headers = {}

    # Our strategy here is to just start downloading chunks while monitoring
    # the Content-Range header to check if we're done. Although we could get
    # the total Content-Length from a HEAD request, not all servers support
    # that (e.g. Minio).
    start = 0
    while True:
        # download a chunk
        stop = start + chunk_size - 1
        headers = {"Range": "bytes={}-{}".format(start, stop), **base_headers}

        response = pool.request(
            "GET",
            url,
            headers=headers,
            timeout=timeout,
        )
        if response.status == HTTPStatus.OK:
            raise ApiException(
                "The file server does not support multipart downloads.",
                status=response.status,
            )
        elif response.status != HTTPStatus.PARTIAL_CONTENT:
            raise ApiException("Unexpected status", status=response.status)

        # write to file
        fileobj.write(response.data)

        # parse content-range header (e.g. "bytes 0-3/7") for next iteration
        content_range = response.headers["Content-Range"]

        start, stop, total = [
            int(x) for x in CONTENT_RANGE_REGEXP.findall(content_range)[0]
        ]

        if callable(callback_func):
            download_bytes: int = total if stop + 1 >= total else stop
            callback_func(download_bytes, total)

        if stop + 1 >= total:
            break
        start += chunk_size

    return total


def upload_file(
    url: str,
    file_path: Path,
    chunk_size: int = 16777216,
    timeout: Optional[Union[float, urllib3.Timeout]] = None,
    pool: Optional[urllib3.PoolManager] = None,
    md5: Optional[bytes] = None,
    callback_func: Optional[Callable[[int, int], None]] = None,
    headers_factory: Optional[Callable[[], Dict[str, str]]] = None,
) -> int:
    """Upload a file at specified file path to a url.

    The upload is accompanied by an MD5 hash so that the file server checks
    the integrity of the file.

    Args:
        url: The url to upload to.
        file_path: The file path to read data from.
        chunk_size: The size of the chunk in the streaming upload. Note that this
            function does not do multipart upload. Default: 16MB.
        timeout: The total timeout in seconds. The default is a connect timeout of
            5 seconds and a read timeout of 10 minutes.
        pool: If not supplied, a default connection pool will be
            created with a retry policy of 3 retries after 1, 2, 4 seconds.
        md5: The MD5 digest (binary) of the file. Supply the MD5 to enable server-side
            integrity check. Note that when using presigned urls in AWS S3, the md5 hash
            should be included in the signing procedure.
        callback_func: optional function used to receive: bytes_uploaded, total_bytes
            for example: def callback(bytes_uploaded: int, total_bytes: int) -> None
        headers_factory: optional function to inject headers

    Returns:
        The total number of uploaded bytes.

    Raises:
        IOError: Raised if the provided file is incompatible or empty.
        ApiException: raised on unexpected server
            responses (HTTP status codes other than 206, 413, 429, 503)
        urllib3.exceptions.HTTPError: various low-level HTTP errors that persist
            after retrying: connection errors, timeouts, decode errors,
            invalid HTTP headers, payload too large (HTTP 413), too many
            requests (HTTP 429), service unavailable (HTTP 503)
    """
    # cast string to Path if necessary
    if isinstance(file_path, str):
        file_path = Path(file_path)

    # open the file
    with file_path.open("rb") as fileobj:
        size = upload_fileobj(
            url,
            fileobj,
            chunk_size=chunk_size,
            timeout=timeout,
            pool=pool,
            md5=md5,
            callback_func=callback_func,
            headers_factory=headers_factory,
        )

    return size


def _iter_chunks(
    fileobj: BinaryIO,
    chunk_size: int,
    callback_func: Optional[Callable[[int], None]] = None,
):
    """Yield chunks from a file stream"""
    uploaded_bytes: int = 0
    assert chunk_size > 0
    while True:
        data = fileobj.read(chunk_size)
        if len(data) == 0:
            break
        uploaded_bytes += chunk_size
        if callable(callback_func):
            callback_func(uploaded_bytes)
        yield data


class _SeekableChunkIterator:
    """A chunk iterator that can be rewinded in case of urllib3 retries."""

    def __init__(
        self,
        fileobj: BinaryIO,
        chunk_size: int,
        callback_func: Optional[Callable[[int], None]] = None,
    ):
        self.fileobj = fileobj
        self.chunk_size = chunk_size
        self.callback_func = callback_func

    def seek(self, pos: int):
        return self.fileobj.seek(pos)

    def tell(self):
        return self.fileobj.tell()

    def __iter__(self):
        return _iter_chunks(self.fileobj, self.chunk_size, self.callback_func)


def upload_fileobj(
    url: str,
    fileobj: BinaryIO,
    chunk_size: int = 16777216,
    timeout: Optional[Union[float, urllib3.Timeout]] = None,
    pool: Optional[urllib3.PoolManager] = None,
    md5: Optional[bytes] = None,
    callback_func: Optional[Callable[[int, int], None]] = None,
    headers_factory: Optional[Callable[[], Dict[str, str]]] = None,
) -> int:
    """Upload a file object to a url.

    The upload is accompanied by an MD5 hash so that the file server checks
    the integrity of the file.

    Args:
        url: The url to upload to.
        fileobj: The (binary) file object to read from.
        chunk_size: The size of the chunk in the streaming upload. Note that this
            function does not do multipart upload. Default: 16MB.
        timeout: The total timeout in seconds. The default is a connect timeout of
            5 seconds and a read timeout of 10 minutes.
        pool: If not supplied, a default connection pool will be
            created with a retry policy of 3 retries after 1, 2, 4 seconds.
        md5: The MD5 digest (binary) of the file. Supply the MD5 to enable server-side
            integrity check. Note that when using presigned urls in AWS S3, the md5 hash
            should be included in the signing procedure.
        callback_func: optional function used to receive: bytes_uploaded, total_bytes
            for example: def callback(bytes_uploaded: int, total_bytes: int) -> None
        headers_factory: optional function to inject headers

    Returns:
        The total number of uploaded bytes.

    Raises:
        IOError: Raised if the provided file is incompatible or empty.
        ApiException: raised on unexpected server
            responses (HTTP status codes other than 206, 413, 429, 503)
        urllib3.exceptions.HTTPError: various low-level HTTP errors that persist
            after retrying: connection errors, timeouts, decode errors,
            invalid HTTP headers, payload too large (HTTP 413), too many
            requests (HTTP 429), service unavailable (HTTP 503)
    """
    # There are two ways to upload in S3 (Minio):
    # - PutObject: put the whole object in one time
    # - multipart upload: requires presigned urls for every part
    # We can only do the first option as we have no other presigned urls.
    # So we take the first option, but we do stream the request body in chunks.

    # We will get hard to understand tracebacks if the fileobj is not
    # in binary mode. So use a trick to see if fileobj is in binary mode:
    if not isinstance(fileobj.read(0), bytes):
        raise IOError(
            "The file object is not in binary mode. Please open with mode='rb'."
        )

    file_size = fileobj.seek(0, 2)  # go to EOF
    if file_size == 0:
        raise IOError("The file object is empty.")

    if pool is None:
        pool = get_pool()

    fileobj.seek(0)

    def callback(uploaded_bytes: int):
        if callable(callback_func):
            if uploaded_bytes > file_size:
                uploaded_bytes = file_size
            callback_func(uploaded_bytes, file_size)

    iterable = _SeekableChunkIterator(
        fileobj,
        chunk_size=chunk_size,
        callback_func=callback,
    )

    # Tested: both Content-Length and Content-MD5 are checked by Minio
    headers = {
        "Content-Length": str(file_size),
    }
    if md5 is not None:
        headers["Content-MD5"] = base64.b64encode(md5).decode()
    if headers_factory is not None:
        headers.update(headers_factory())
    response = pool.request(
        "PUT",
        url,
        body=iterable,
        headers=headers,
        timeout=DEFAULT_UPLOAD_TIMEOUT if timeout is None else timeout,
    )
    if response.status not in {HTTPStatus.OK, HTTPStatus.CREATED}:
        raise ApiException("Unexpected status", status=response.status)

    return file_size
