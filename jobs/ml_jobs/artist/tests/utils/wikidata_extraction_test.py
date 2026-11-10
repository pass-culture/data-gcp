"""Tests for src/extraction/wikidata_extraction.py — no network calls needed.

extract_query's fetch/retry behavior is exercised through
src/extraction/qlever.py, which it calls into — see tests/utils/qlever_test.py —
and through tests/extract_from_wikidata_test.py's `extract` tests.
"""

from unittest.mock import patch

import pandas as pd

from src.extraction.constants import IdProperty
from src.extraction.wikidata_config import (
    QUERY_CONFIGS,
    SHARDS,
    QueryConfig,
    render_query,
)
from src.extraction.wikidata_extraction import extract_query, extract_wikidata_id

ENTITY_PREFIX = "http://www.wikidata.org/entity/"


def _fake_fetch(sparql_query: str, dtype: dict) -> pd.DataFrame:
    """One row per call, whose Q-id ends with the shard digit in the query."""
    for shard in SHARDS:
        if f'STRENDS(STR(?wikidata_id), "{shard}")' in sparql_query:
            return pd.DataFrame({"wikidata_id": [f"{ENTITY_PREFIX}Q1{shard}"]})
    return pd.DataFrame({"wikidata_id": [f"{ENTITY_PREFIX}Q42"]})


class TestShardedRendering:
    def test_unsharded_query_has_no_shard_filter(self):
        assert "STRENDS" not in render_query("book")

    def test_shard_filter_applies_to_every_entity_filter(self):
        unsharded = render_query("book")
        sharded = render_query("book", shard="3")
        shard_filter = 'FILTER(STRENDS(STR(?wikidata_id), "3"))'
        assert sharded.count(shard_filter) == unsharded.count("VALUES ?type")
        assert sharded.count(shard_filter) > 1

    def test_ids_template_supports_shard(self):
        assert 'STRENDS(STR(?wikidata_id), "7")' in render_query("gkg", shard="7")


class TestExtractQuery:
    def test_sharded_target_fetches_and_concatenates_every_shard(self):
        assert QUERY_CONFIGS["book"].sharded
        with patch(
            "src.extraction.wikidata_extraction.fetch_wikidata_qlever_csv",
            side_effect=_fake_fetch,
        ) as mock_fetch:
            df = extract_query("book")
        assert mock_fetch.call_count == len(SHARDS)
        assert list(df["wikidata_id"]) == [f"Q1{shard}" for shard in SHARDS]

    def test_unsharded_target_fetches_once(self):
        assert not QUERY_CONFIGS["gkg"].sharded
        with patch(
            "src.extraction.wikidata_extraction.fetch_wikidata_qlever_csv",
            side_effect=_fake_fetch,
        ) as mock_fetch:
            df = extract_query("gkg")
        assert mock_fetch.call_count == 1
        assert list(df["wikidata_id"]) == ["Q42"]


class TestIdColumnDtypes:
    def test_reads_every_id_property_as_str(self):
        config = QueryConfig(
            template="extract_artists.rq.j2",
            entity_types=["wd:Q5"],
            id_properties=[
                IdProperty("deezer_id", "wdt:P2722"),
                IdProperty("isni_id", "wdt:P213", filter=False),
            ],
        )
        assert config.id_column_dtypes == {"deezer_id": str, "isni_id": str}


class TestExtractWikidataId:
    def test_strips_wikidata_uri_prefix(self):
        df = pd.DataFrame(
            {
                "wikidata_id": [
                    "https://www.wikidata.org/entity/Q42",
                    "https://www.wikidata.org/entity/Q123",
                ]
            }
        )
        result = extract_wikidata_id(df)
        assert list(result["wikidata_id"]) == ["Q42", "Q123"]

    def test_does_not_alter_other_columns(self):
        df = pd.DataFrame(
            {
                "wikidata_id": ["https://www.wikidata.org/entity/Q1"],
                "artist_name_fr": ["Test"],
            }
        )
        result = extract_wikidata_id(df)
        assert result["artist_name_fr"].iloc[0] == "Test"
