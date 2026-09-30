import pandas as pd

from assign_linked_ids import (
    build_graph_and_assign_ids,
    post_process_graph_matching,
)


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
