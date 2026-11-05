import random
from unittest.mock import patch

import pandas as pd

from assign_linked_ids import (
    assign_cluster_id,
    build_graph_and_assign_ids,
    post_process_graph_matching,
)


def _cluster_ids(edges: list[tuple[str, str]]) -> dict[str, str]:
    """Run the clustering on edges and return {item_id: cluster_id} for all members."""
    linked_offers = pd.DataFrame(
        edges, columns=["item_id_synchro", "item_id_candidate"]
    )
    result = build_graph_and_assign_ids(linked_offers)
    return {
        **dict(zip(result["item_id_synchro"], result["new_item_id"])),
        **dict(zip(result["item_id_candidate"], result["new_item_id"])),
    }


class TestAssignClusterId:
    def test_id_is_prefixed_min_offer_id(self):
        assert (
            assign_cluster_id({"offer-30", "offer-12", "offer-50"})
            == "item_cluster_o12"
        )

    def test_offer_ids_are_compared_numerically(self):
        assert assign_cluster_id({"offer-9", "offer-10"}) == "item_cluster_o9"

    def test_numeric_offer_wins_over_unexpected_ids(self):
        assert assign_cluster_id({"link-abc", "offer-99"}) == "item_cluster_o99"

    def test_unexpected_ids_fall_back_to_string_order_and_warn(self):
        with patch("assign_linked_ids.logger") as mock_logger:
            assert assign_cluster_id({"s2", "s1"}) == "item_cluster_os1"
        mock_logger.warning.assert_called_once()

    def test_numeric_anchor_does_not_warn(self):
        with patch("assign_linked_ids.logger") as mock_logger:
            assign_cluster_id({"link-abc", "offer-99"})
        mock_logger.warning.assert_not_called()


class TestDeterministicClusterIds:
    EDGES = [
        ("offer-10", "offer-20"),
        ("offer-20", "offer-30"),
        ("offer-40", "offer-50"),
        ("offer-50", "offer-60"),
    ]

    def test_ids_follow_oldest_member(self):
        ids = _cluster_ids(self.EDGES)
        assert {ids[o] for o in ("offer-10", "offer-20", "offer-30")} == {
            "item_cluster_o10"
        }
        assert {ids[o] for o in ("offer-40", "offer-50", "offer-60")} == {
            "item_cluster_o40"
        }

    def test_ids_do_not_depend_on_row_order_or_edge_direction(self):
        shuffled_and_reversed = [(b, a) for a, b in self.EDGES]
        random.Random(42).shuffle(shuffled_and_reversed)
        assert shuffled_and_reversed != [(b, a) for a, b in self.EDGES]
        assert _cluster_ids(shuffled_and_reversed) == _cluster_ids(self.EDGES)

    def test_new_offer_joining_keeps_cluster_id(self):
        ids = _cluster_ids(self.EDGES + [("offer-30", "offer-99")])
        assert ids["offer-99"] == "item_cluster_o10"
        assert ids["offer-10"] == "item_cluster_o10"

    def test_merge_keeps_oldest_anchor(self):
        ids = _cluster_ids(self.EDGES + [("offer-30", "offer-40")])
        assert set(ids.values()) == {"item_cluster_o10"}


class TestBuildGraphAndAssignIds:
    def test_connected_items_share_a_cluster_id(self):
        # s1-c1 and s2-c1 are connected through c1 -> same cluster.
        linked_offers = pd.DataFrame(
            {
                "item_id_synchro": ["s1", "s2"],
                "item_id_candidate": ["c1", "c1"],
            }
        )
        result = build_graph_and_assign_ids(linked_offers)
        assert result["new_item_id"].nunique() == 1
        assert result["new_item_id"].iloc[0].startswith("item_cluster_")

    def test_singleton_clusters_are_dropped(self):
        # s3-c3 forms an isolated pair whose cluster only spans one synchro row.
        linked_offers = pd.DataFrame(
            {
                "item_id_synchro": ["s1", "s2", "s3"],
                "item_id_candidate": ["c1", "c1", "c3"],
            }
        )
        result = build_graph_and_assign_ids(linked_offers)
        assert result["item_id_synchro"].tolist() == ["s1", "s2"]

    def test_self_links_are_dropped(self):
        linked_offers = pd.DataFrame(
            {
                "item_id_synchro": ["s1", "s2", "x1"],
                "item_id_candidate": ["c1", "c1", "x1"],
            }
        )
        result = build_graph_and_assign_ids(linked_offers)
        assert "x1" not in result["item_id_synchro"].tolist()


class TestPostProcessGraphMatching:
    def test_removes_self_links(self):
        linked = pd.DataFrame(
            {
                "item_id_synchro": ["s1", "x1"],
                "item_id_candidate": ["c1", "x1"],
                "new_item_id": ["item_cluster_0", "item_cluster_1"],
            }
        )
        result = post_process_graph_matching(linked)
        assert "x1" not in result["item_id_candidate"].tolist()

    def test_melts_synchro_and_candidate_into_single_column(self):
        linked = pd.DataFrame(
            {
                "item_id_synchro": ["s1", "s2"],
                "item_id_candidate": ["c1", "c1"],
                "new_item_id": ["item_cluster_0", "item_cluster_0"],
            }
        )
        result = post_process_graph_matching(linked)
        assert set(result.columns) == {"item_id_candidate", "new_item_id"}
        # s1, s2 (synchro side) + c1 (candidate side, deduplicated).
        assert sorted(result["item_id_candidate"].tolist()) == ["c1", "s1", "s2"]
        assert result["new_item_id"].nunique() == 1
