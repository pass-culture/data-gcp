import pandas as pd

from src.delta import build_removed_events, match_offers_to_existing_events
from src.interfaces import ActionType, CommentType


def _make_matched_pairs_df(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "offer_id_1",
            "offer_id_2",
            "name_similarity",
            "description_similarity",
            "image_similarity",
        ],
    )


def _make_link_df(rows):
    return pd.DataFrame(rows, columns=["event_id", "offer_id"])


class TestMatchOffersToExistingEvents:
    def test_new_offer_matches_one_series(self):
        matched_pairs_df = _make_matched_pairs_df([["new_A", "old_B", 95, 0, 0]])
        link_df = _make_link_df([["S1", "old_B"]])

        result = match_offers_to_existing_events(matched_pairs_df, link_df)

        assert len(result) == 1
        row = result.iloc[0]
        assert row["offer_id"] == "new_A"
        assert row["event_id"] == "S1"
        assert row["action"] == ActionType.ADD
        assert row["comment"] == CommentType.LINKED_TO_EXISTING_EVENT

    def test_new_offer_matches_via_reversed_columns(self):
        # old offer is offer_id_1, new offer is offer_id_2
        matched_pairs_df = _make_matched_pairs_df([["old_B", "new_A", 95, 0, 0]])
        link_df = _make_link_df([["S1", "old_B"]])

        result = match_offers_to_existing_events(matched_pairs_df, link_df)

        assert len(result) == 1
        assert result.iloc[0]["offer_id"] == "new_A"
        assert result.iloc[0]["event_id"] == "S1"

    def test_majority_vote_wins_over_single_high_score(self):
        # new_A matches 2 offers of S1 (scores 92, 91) and 1 offer of S2 (score 99)
        matched_pairs_df = _make_matched_pairs_df(
            [
                ["new_A", "s1_offer_1", 92, 0, 0],
                ["new_A", "s1_offer_2", 91, 0, 0],
                ["new_A", "s2_offer_1", 99, 0, 0],
            ]
        )
        link_df = _make_link_df(
            [
                ["S1", "s1_offer_1"],
                ["S1", "s1_offer_2"],
                ["S2", "s2_offer_1"],
            ]
        )

        result = match_offers_to_existing_events(matched_pairs_df, link_df)

        assert len(result) == 1
        assert result.iloc[0]["event_id"] == "S1"

    def test_score_tie_break_when_counts_equal(self):
        # new_A matches 1 offer of S1 (score 99) and 1 offer of S2 (score 92)
        matched_pairs_df = _make_matched_pairs_df(
            [
                ["new_A", "s1_offer_1", 99, 0, 0],
                ["new_A", "s2_offer_1", 92, 0, 0],
            ]
        )
        link_df = _make_link_df(
            [
                ["S1", "s1_offer_1"],
                ["S2", "s2_offer_1"],
            ]
        )

        result = match_offers_to_existing_events(matched_pairs_df, link_df)

        assert len(result) == 1
        assert result.iloc[0]["event_id"] == "S1"

    def test_event_id_tie_break_when_count_and_score_equal(self):
        matched_pairs_df = _make_matched_pairs_df(
            [
                ["new_A", "s1_offer_1", 95, 0, 0],
                ["new_A", "s2_offer_1", 95, 0, 0],
            ]
        )
        link_df = _make_link_df(
            [
                ["S2", "s2_offer_1"],
                ["S1", "s1_offer_1"],
            ]
        )

        result = match_offers_to_existing_events(matched_pairs_df, link_df)

        assert len(result) == 1
        assert result.iloc[0]["event_id"] == "S1"

    def test_edge_between_two_linked_offers_is_ignored(self):
        matched_pairs_df = _make_matched_pairs_df([["old_A", "old_B", 95, 0, 0]])
        link_df = _make_link_df([["S1", "old_A"], ["S2", "old_B"]])

        result = match_offers_to_existing_events(matched_pairs_df, link_df)

        assert len(result) == 0

    def test_edge_between_two_new_offers_is_not_linked(self):
        matched_pairs_df = _make_matched_pairs_df([["new_A", "new_B", 95, 0, 0]])
        link_df = _make_link_df([["S1", "old_C"]])

        result = match_offers_to_existing_events(matched_pairs_df, link_df)

        assert len(result) == 0

    def test_image_similarity_is_scaled_for_scoring(self):
        # image_similarity 0.99 -> score 99, should win over name/description 92
        matched_pairs_df = _make_matched_pairs_df(
            [
                ["new_A", "s1_offer_1", 92, 0, 0],
                ["new_A", "s2_offer_1", 0, 0, 0.99],
            ]
        )
        link_df = _make_link_df(
            [
                ["S1", "s1_offer_1"],
                ["S2", "s2_offer_1"],
            ]
        )

        result = match_offers_to_existing_events(matched_pairs_df, link_df)

        assert len(result) == 1
        assert result.iloc[0]["event_id"] == "S2"


class TestBuildRemovedEvents:
    def test_series_with_all_offers_deleted_is_removed(self):
        link_df = _make_link_df([["S1", "A"], ["S1", "B"]])
        active_offer_ids = {"C", "D"}

        removed_events_df, removed_links_df = build_removed_events(
            link_df, active_offer_ids
        )

        assert len(removed_events_df) == 1
        assert removed_events_df.iloc[0]["event_id"] == "S1"
        assert removed_events_df.iloc[0]["action"] == ActionType.REMOVE
        assert removed_events_df.iloc[0]["comment"] == CommentType.REMOVED_EVENT

        assert len(removed_links_df) == 2
        assert set(removed_links_df["offer_id"]) == {"A", "B"}
        assert (removed_links_df["action"] == ActionType.REMOVE).all()
        assert (removed_links_df["comment"] == CommentType.REMOVED_EVENT).all()

    def test_series_with_some_active_offers_is_kept(self):
        link_df = _make_link_df([["S1", "A"], ["S1", "B"]])
        active_offer_ids = {"A"}

        removed_events_df, removed_links_df = build_removed_events(
            link_df, active_offer_ids
        )

        assert len(removed_events_df) == 0
        assert len(removed_links_df) == 0

    def test_offer_absent_from_similarities_but_present_in_raw_offers_not_deleted(
        self,
    ):
        # Simulates an offer with no similarity pair (absent from similarities
        # file) but still active (present in the raw offers input).
        link_df = _make_link_df([["S1", "A"]])
        active_offer_ids = {"A"}

        removed_events_df, removed_links_df = build_removed_events(
            link_df, active_offer_ids
        )

        assert len(removed_events_df) == 0
        assert len(removed_links_df) == 0

    def test_no_events_removed_when_all_offers_active(self):
        link_df = _make_link_df([["S1", "A"], ["S2", "B"]])
        active_offer_ids = {"A", "B"}

        removed_events_df, removed_links_df = build_removed_events(
            link_df, active_offer_ids
        )

        assert len(removed_events_df) == 0
        assert len(removed_links_df) == 0
