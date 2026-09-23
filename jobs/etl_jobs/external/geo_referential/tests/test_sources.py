from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from utils import config
from utils.sources import download


def _response(
    chunks: list[bytes], content_length: int | None, status_code: int = 200
) -> MagicMock:
    response = MagicMock()
    response.__enter__.return_value = response
    response.status_code = status_code
    response.headers = (
        {} if content_length is None else {"Content-Length": str(content_length)}
    )
    response.iter_content.return_value = chunks
    return response


def test_download_resumes_a_truncated_download(tmp_path: Path):
    dest = tmp_path / "file.bin"
    truncated = _response([b"12345"], content_length=10)
    rest = _response([b"67890"], content_length=5, status_code=206)

    with patch("utils.sources.requests.get", side_effect=[truncated, rest]) as get:
        download("https://example.test/file.bin", dest)

    assert get.call_count == 2
    assert get.call_args_list[0].kwargs["headers"] == {}
    assert get.call_args_list[1].kwargs["headers"] == {"Range": "bytes=5-"}
    assert dest.read_bytes() == b"1234567890"


def test_download_restarts_when_the_server_ignores_the_range_request(tmp_path: Path):
    dest = tmp_path / "file.bin"
    truncated = _response([b"12345"], content_length=10)
    full = _response([b"1234567890"], content_length=10)

    with patch("utils.sources.requests.get", side_effect=[truncated, full]):
        download("https://example.test/file.bin", dest)

    assert dest.read_bytes() == b"1234567890"


def test_download_raises_when_every_attempt_is_truncated(tmp_path: Path):
    dest = tmp_path / "file.bin"
    truncated = [
        _response([b"12345"], content_length=10)
        for _ in range(config.DOWNLOAD_ATTEMPTS)
    ]

    with patch("utils.sources.requests.get", side_effect=truncated):
        with pytest.raises(OSError, match="truncated download"):
            download("https://example.test/file.bin", dest)


def test_download_retries_a_broken_connection(tmp_path: Path):
    dest = tmp_path / "file.bin"
    complete = _response([b"1234567890"], content_length=10)
    side_effect = [requests.ConnectionError("connection broken"), complete]

    with patch("utils.sources.requests.get", side_effect=side_effect) as get:
        download("https://example.test/file.bin", dest)

    assert get.call_count == 2
    assert dest.read_bytes() == b"1234567890"


def test_download_accepts_a_response_without_content_length(tmp_path: Path):
    dest = tmp_path / "file.bin"

    with patch("utils.sources.requests.get", side_effect=[_response([b"123"], None)]):
        download("https://example.test/file.bin", dest)

    assert dest.read_bytes() == b"123"
