"""Tests for src/utils/qlever.py — no network calls needed."""

from unittest.mock import Mock, patch

import pytest
import requests

from src.utils.qlever import (
    QLeverQueryTooExpensive,
    _clear_qlever_cache_once,
    _is_cost_rejection,
    clear_qlever_cache,
    fetch_wikidata_qlever_csv,
    fetch_wikidata_qlever_csv_batch,
)


def _mock_response(status_code: int, text: str = "", json_data=None) -> Mock:
    response = Mock(spec=requests.Response)
    response.status_code = status_code
    response.text = text
    if json_data is not None:
        response.json.return_value = json_data
    else:
        response.json.side_effect = ValueError("no JSON body")
    return response


@pytest.fixture(autouse=True)
def _no_real_sleep():
    """Tenacity's default sleep strategy is a real time.sleep — patch it out so
    tests exercising the retry decorator (stop_after_attempt(3), exponential
    backoff up to 40s) don't actually block for tens of seconds."""
    with patch("tenacity.nap.time.sleep"):
        yield


class TestQleverRetry:
    """src/utils/qlever.py's qlever_retry (tenacity) decorates every QLever HTTP
    call: 3 attempts, exponential backoff, retrying only on
    requests.RequestException — see qlever_retry's docstring/comment."""

    def test_clear_cache_once_raises_after_exhausting_retries(self):
        with (
            patch(
                "src.utils.qlever.requests.get",
                return_value=_mock_response(500, "boom"),
            ) as mock_get,
            pytest.raises(requests.RequestException),
        ):
            _clear_qlever_cache_once()
        assert mock_get.call_count == 3

    def test_clear_cache_succeeds_without_retry(self):
        with patch(
            "src.utils.qlever.requests.get",
            return_value=_mock_response(200),
        ) as mock_get:
            _clear_qlever_cache_once()
        assert mock_get.call_count == 1

    def test_clear_qlever_cache_swallows_failure_after_retries(self):
        """The outer clear_qlever_cache must NOT raise even after all retries are
        exhausted — cache clearing is best-effort, extraction should proceed."""
        with patch(
            "src.utils.qlever.requests.get",
            return_value=_mock_response(500, "boom"),
        ) as mock_get:
            clear_qlever_cache()  # must not raise
        assert mock_get.call_count == 3

    def test_fetch_csv_retries_then_succeeds(self):
        responses = [
            _mock_response(500, "transient"),
            _mock_response(500, "transient"),
            _mock_response(200, "wikidata_id\nQ1\n"),
        ]
        with patch(
            "src.utils.qlever.requests.post", side_effect=responses
        ) as mock_post:
            df = fetch_wikidata_qlever_csv("SELECT ...")
        assert mock_post.call_count == 3
        assert list(df["wikidata_id"]) == ["Q1"]

    def test_fetch_csv_raises_after_exhausting_retries(self):
        with (
            patch(
                "src.utils.qlever.requests.post",
                return_value=_mock_response(500, "boom"),
            ) as mock_post,
            pytest.raises(requests.RequestException),
        ):
            fetch_wikidata_qlever_csv("SELECT ...")
        assert mock_post.call_count == 3

    def test_fetch_csv_batch_raises_cost_rejection_without_retrying(self):
        """A genuine cost rejection (QLeverQueryTooExpensive) must propagate
        immediately, on the first attempt — retrying an identical too-expensive
        query would just fail the same way every time; the caller bisects
        instead (see src.utils.wikidata_extraction.hydrate_batch)."""
        response = _mock_response(429, json_data={"exception": "Query timed out"})
        with (
            patch("src.utils.qlever.requests.post", return_value=response) as mock_post,
            pytest.raises(QLeverQueryTooExpensive),
        ):
            fetch_wikidata_qlever_csv_batch("SELECT ...")
        assert mock_post.call_count == 1

    def test_fetch_csv_batch_retries_transient_errors(self):
        responses = [
            _mock_response(503, "transient"),
            _mock_response(200, "wikidata_id\nQ1\n"),
        ]
        with patch(
            "src.utils.qlever.requests.post", side_effect=responses
        ) as mock_post:
            df = fetch_wikidata_qlever_csv_batch("SELECT ...")
        assert mock_post.call_count == 2
        assert list(df["wikidata_id"]) == ["Q1"]


class TestIsCostRejection:
    def test_true_for_429_with_timeout_phrase(self):
        response = _mock_response(429, json_data={"exception": "Query timed out"})
        assert _is_cost_rejection(response) is True

    def test_false_for_429_without_recognized_phrase(self):
        response = _mock_response(429, json_data={"exception": "rate limited"})
        assert _is_cost_rejection(response) is False

    def test_false_for_non_429_status(self):
        response = _mock_response(500, json_data={"exception": "timed out"})
        assert _is_cost_rejection(response) is False

    def test_false_when_body_is_not_json(self):
        response = _mock_response(429, text="not json")
        assert _is_cost_rejection(response) is False
