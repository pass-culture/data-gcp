import pandas as pd

from constants import SYNCHRO_SUBCATEGORIES
from prepare_tables import filter_candidates


def _sample_data():
    return pd.DataFrame(
        {
            "item_id": ["a", "b", "c", "d"],
            "offer_subcategory_id": [
                SYNCHRO_SUBCATEGORIES[0],
                SYNCHRO_SUBCATEGORIES[1],
                "SPECTACLE_REPRESENTATION",
                "CONCERT",
            ],
        }
    )


class TestFilterCandidates:
    def test_product_keeps_only_synchro_subcategories(self):
        result = filter_candidates("product", _sample_data())
        assert result["item_id"].tolist() == ["a", "b"]

    def test_offer_keeps_non_synchro_subcategories(self):
        result = filter_candidates("offer", _sample_data())
        assert result["item_id"].tolist() == ["c", "d"]

    def test_offer_appends_unmatched_candidates(self):
        unmatched = pd.DataFrame({"item_id": ["a"]})
        result = filter_candidates("offer", _sample_data(), unmatched)
        assert sorted(result["item_id"].tolist()) == ["a", "c", "d"]

    def test_unknown_linkage_type_returns_data_unchanged(self):
        data = _sample_data()
        result = filter_candidates("something_else", data)
        assert result["item_id"].tolist() == data["item_id"].tolist()
