from src.clustering.uuid_matching import (
    cluster_item_sets,
    jaccard_similarity,
    match_uuids,
    outlier_uuid,
)


def test_jaccard_similarity_basic() -> None:
    assert jaccard_similarity({"1", "2"}, {"2", "3"}) == 1 / 3


def test_jaccard_similarity_identical_sets_is_one() -> None:
    assert jaccard_similarity({"1", "2"}, {"1", "2"}) == 1.0


def test_jaccard_similarity_both_empty_is_zero() -> None:
    assert jaccard_similarity(set(), set()) == 0.0


def test_cluster_item_sets_excludes_outliers() -> None:
    groups = cluster_item_sets(["1", "2", "3"], [0, 0, -1])
    assert groups == {0: {"1", "2"}}


def test_outlier_uuid_is_stable_per_corpus() -> None:
    assert outlier_uuid("01000000") == outlier_uuid("01000000")


def test_outlier_uuid_differs_per_corpus() -> None:
    assert outlier_uuid("01000000") != outlier_uuid("02000000")


def test_match_uuids_first_run_assigns_fresh_uuids() -> None:
    new_clusters = {0: {"1", "2"}, 1: {"3", "4"}}
    assignments, monitoring = match_uuids({}, new_clusters, threshold=0.5)

    assert len(assignments) == 2
    assert len(set(assignments.values())) == 2
    assert all(row["old_uuid"] is None for row in monitoring)


def test_match_uuids_keeps_uuid_for_unchanged_cluster() -> None:
    old_clusters = {"uuid-a": {"1", "2", "3"}}
    new_clusters = {0: {"1", "2", "3"}}

    assignments, monitoring = match_uuids(old_clusters, new_clusters, threshold=0.5)

    assert assignments[0] == "uuid-a"
    assert monitoring[0]["jaccard"] == 1.0


def test_match_uuids_renews_uuid_below_threshold() -> None:
    old_clusters = {"uuid-a": {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}}
    new_clusters = {0: {"1", "9"}}  # jaccard = 2/10 = 0.2, below 0.5

    assignments, _ = match_uuids(old_clusters, new_clusters, threshold=0.5)

    assert assignments[0] != "uuid-a"


def test_match_uuids_requires_mutual_best_match() -> None:
    # uuid-a's best match is new cluster 0, but 0's own best match is uuid-b
    # (higher jaccard) -- not mutual, so 0 inherits uuid-b, not uuid-a.
    old_clusters = {"uuid-a": {"1", "2"}, "uuid-b": {"1", "2", "3", "4"}}
    new_clusters = {0: {"1", "2", "3", "4"}}

    assignments, _ = match_uuids(old_clusters, new_clusters, threshold=0.1)

    assert assignments[0] == "uuid-b"


def test_match_uuids_renamed_split_cluster_gets_fresh_uuid() -> None:
    old_clusters = {"uuid-a": {"1", "2", "3", "4"}}
    new_clusters = {0: {"1", "2"}, 1: {"3", "4"}}

    assignments, _ = match_uuids(old_clusters, new_clusters, threshold=0.5)

    # Both halves tie at jaccard 0.5; only one inherits "uuid-a" as the
    # (arbitrarily broken tie) mutual best match, the other gets a fresh one.
    assert len(set(assignments.values())) == 2
