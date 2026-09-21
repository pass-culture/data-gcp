import pandas as pd

from constants import RETRIEVAL_FILTERS
from linkage_candidates import build_filter_dict


class TestBuildFilterDict:
    def test_selects_only_requested_keys(self):
        row = pd.Series(
            {"edition": "3", "offer_subcategory_id": "LIVRE_PAPIER", "other": "z"}
        )
        result = build_filter_dict(row, ["edition", "offer_subcategory_id"])
        assert result == {"edition": "3", "offer_subcategory_id": "LIVRE_PAPIER"}

    def test_works_with_retrieval_filters_constant(self):
        row = pd.Series({f: f"val_{f}" for f in RETRIEVAL_FILTERS})
        result = build_filter_dict(row, RETRIEVAL_FILTERS)
        assert set(result.keys()) == set(RETRIEVAL_FILTERS)

    def test_empty_filter_list_returns_empty_dict(self):
        row = pd.Series({"edition": "3"})
        assert build_filter_dict(row, []) == {}
