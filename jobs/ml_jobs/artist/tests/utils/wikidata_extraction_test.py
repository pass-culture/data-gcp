"""Tests for src/extraction/wikidata_extraction.py — no network calls needed.

fetch_discovery/hydrate_batch's own behavior (retry, bisection, cost-rejection
handling) is exercised through src/extraction/qlever.py, which they call into —
see tests/utils/qlever_test.py — and through
tests/extract_from_wikidata_test.py's end-to-end checkpoint-resume test.
"""

import pandas as pd

from src.extraction.wikidata_extraction import extract_wikidata_id


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
