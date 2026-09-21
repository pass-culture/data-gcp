import pandas as pd

from constants import MATCHING_FEATURES
from link_items import (
    _chunkify,
    extract_unmatched_elements,
    get_links,
    postprocess_matching,
    setup_matching,
)


class TestSetupMatching:
    def test_builds_one_comparator_per_feature(self):
        comparator = setup_matching("product")
        assert len(comparator.features) == len(MATCHING_FEATURES["product"])

    def test_supports_both_linkage_types(self):
        for linkage_type in ("product", "offer"):
            comparator = setup_matching(linkage_type)
            assert len(comparator.features) == len(MATCHING_FEATURES[linkage_type])


class TestGetLinks:
    def test_returns_only_pairs_above_threshold(self):
        comparator = setup_matching("product")
        left = pd.DataFrame({"oeuvre": ["naruto", "bleach"]})
        right = pd.DataFrame({"oeuvre": ["naruto", "onepiece"]})
        candidate_links = pd.MultiIndex.from_tuples(
            [(0, 0), (1, 1), (0, 1)], names=["level_0", "level_1"]
        )

        result = get_links(candidate_links, comparator, "product", left, right)

        assert result[["index_1", "index_2"]].values.tolist() == [[0, 0]]

    def test_renames_columns(self):
        comparator = setup_matching("product")
        left = pd.DataFrame({"oeuvre": ["naruto"]})
        right = pd.DataFrame({"oeuvre": ["naruto"]})
        candidate_links = pd.MultiIndex.from_tuples(
            [(0, 0)], names=["level_0", "level_1"]
        )

        result = get_links(candidate_links, comparator, "product", left, right)

        assert set(result.columns) == {"index_1", "index_2", "oeuvre_score"}


class TestChunkify:
    def test_splits_into_balanced_chunks(self):
        assert _chunkify([1, 2, 3, 4, 5], 2) == [[1, 3, 5], [2, 4]]

    def test_chunk_sizes_are_balanced(self):
        chunks = _chunkify(list(range(10)), 3)
        assert [len(c) for c in chunks] == [4, 3, 3]

    def test_more_chunks_than_elements_yields_empty_chunks(self):
        assert _chunkify([1, 2], 3) == [[1], [2], []]


class TestPostprocessMatching:
    def _run(self, matches):
        candidates = pd.DataFrame({"item_id_candidate": ["cand0", "cand1"]})
        sources = pd.DataFrame({"item_id_synchro": ["syn0", "syn1"]})
        return postprocess_matching(matches, candidates, sources)

    def test_maps_indices_back_to_item_ids(self):
        matches = pd.DataFrame({"index_1": [0], "index_2": [0], "oeuvre_score": [1]})
        final, _ = self._run(matches)
        row = final.iloc[0]
        assert row["item_id_candidate"] == "cand0"
        assert row["item_id_synchro"] == "syn0"

    def test_keeps_only_max_score_per_candidate(self):
        matches = pd.DataFrame(
            {
                "index_1": [0, 0],
                "index_2": [0, 1],
                "oeuvre_score": [1, 0],
            }
        )
        final, _ = self._run(matches)
        assert final["oeuvre_score"].tolist() == [1]
        assert final["item_id_synchro"].tolist() == ["syn0"]

    def test_counts_candidates_with_multiple_matches(self):
        matches = pd.DataFrame(
            {
                "index_1": [0, 0, 1],
                "index_2": [0, 1, 0],
                "oeuvre_score": [1, 1, 1],
            }
        )
        final, num_duplicate_matches = self._run(matches)
        # cand0 is matched to both syn0 and syn1 -> one duplicated candidate.
        assert num_duplicate_matches == 1
        assert len(final) == 3

    def test_drops_duplicate_synchro_candidate_pairs(self):
        matches = pd.DataFrame(
            {
                "index_1": [0, 0],
                "index_2": [0, 0],
                "oeuvre_score": [1, 1],
            }
        )
        final, _ = self._run(matches)
        assert len(final) == 1


class TestExtractUnmatchedElements:
    def test_returns_candidates_without_a_match(self):
        candidates = pd.DataFrame({"item_id": ["a", "b", "c"]})
        output = pd.DataFrame({"item_id_candidate": ["a"]})

        unmatched = extract_unmatched_elements(candidates, output)

        assert sorted(unmatched["item_id"].tolist()) == ["b", "c"]

    def test_empty_when_all_matched(self):
        candidates = pd.DataFrame({"item_id": ["a", "b"]})
        output = pd.DataFrame({"item_id_candidate": ["a", "b"]})

        unmatched = extract_unmatched_elements(candidates, output)

        assert unmatched.empty
