"""Tests for src/extraction/qlever.py — no network calls needed."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from src.extraction.qlever import fetch_wikidata_qlever_csv


def _mock_response(status_code: int, text: str = "") -> Mock:
    response = Mock(spec=requests.Response)
    response.status_code = status_code
    response.text = text
    return response


@pytest.fixture(autouse=True)
def _no_real_sleep():
    """Tenacity's default sleep strategy is a real time.sleep — patch it out so
    tests exercising the retry decorator (stop_after_attempt(4), exponential
    backoff up to 240s) don't actually block for minutes."""
    with patch("tenacity.nap.time.sleep"):
        yield


class TestQleverRetry:
    """src/extraction/qlever.py's qlever_retry (tenacity) decorates every QLever HTTP
    call: 4 attempts, exponential backoff, retrying on requests.RequestException
    except 4xx responses other than 429."""

    def test_backoff_schedule(self):
        wait = fetch_wikidata_qlever_csv.retry.wait
        waits = [wait(Mock(attempt_number=n)) for n in range(1, 5)]
        assert waits == [30, 60, 120, 240]

    def test_fetch_csv_fails_fast_on_client_error(self):
        with (
            patch(
                "src.extraction.qlever.requests.post",
                return_value=_mock_response(400, "bad query"),
            ) as mock_post,
            pytest.raises(requests.RequestException),
        ):
            fetch_wikidata_qlever_csv("SELECT ...", dtype={})
        assert mock_post.call_count == 1

    def test_fetch_csv_retries_then_succeeds(self):
        responses = [
            _mock_response(500, "transient"),
            _mock_response(500, "transient"),
            _mock_response(200, "wikidata_id\nQ1\n"),
        ]
        with patch(
            "src.extraction.qlever.requests.post", side_effect=responses
        ) as mock_post:
            df = fetch_wikidata_qlever_csv("SELECT ...", dtype={})
        assert mock_post.call_count == 3
        assert list(df["wikidata_id"]) == ["Q1"]

    def test_fetch_csv_applies_dtype(self):
        """Pinned dtypes must override read_csv's inference, so numeric-looking
        IDs stay strings instead of becoming numbers."""
        with patch(
            "src.extraction.qlever.requests.post",
            return_value=_mock_response(200, "wikidata_id,deezer_id\nQ1,123\nQ2,\n"),
        ):
            df = fetch_wikidata_qlever_csv("SELECT ...", dtype={"deezer_id": str})
        assert df["deezer_id"].iloc[0] == "123"
        assert pd.isna(df["deezer_id"].iloc[1])

    def test_fetch_csv_raises_after_exhausting_retries(self):
        with (
            patch(
                "src.extraction.qlever.requests.post",
                return_value=_mock_response(500, "boom"),
            ) as mock_post,
            pytest.raises(requests.RequestException),
        ):
            fetch_wikidata_qlever_csv("SELECT ...", dtype={})
        assert mock_post.call_count == 4

    def test_fetch_csv_recovers_from_a_sustained_rate_limit_burst(self):
        """Regression for failure_log.txt: QLever's nginx front-end can 429 a
        burst of requests for several attempts in a row before recovering. The old 3-attempt policy
        exhausted retries on exactly this pattern and crashed the whole extract
        task; 4 attempts must ride out 3 consecutive 429s."""
        rate_limited = _mock_response(429, text="<html>429 Too Many Requests</html>")
        responses = [rate_limited, rate_limited, rate_limited]
        responses.append(_mock_response(200, "wikidata_id\nQ1\n"))
        with patch(
            "src.extraction.qlever.requests.post", side_effect=responses
        ) as mock_post:
            df = fetch_wikidata_qlever_csv("SELECT ...", dtype={})
        assert mock_post.call_count == 4
        assert list(df["wikidata_id"]) == ["Q1"]
