"""Topic UUID carryover between refits (spec section 7.3): a new cluster
keeps an old UUID only if they're each other's best Jaccard match, above
``uuid_match_jaccard``; otherwise it gets a fresh UUID. The outlier group
keeps one fixed UUID per corpus, derived deterministically so it needs no
extra storage to stay stable across refits.
"""

import uuid

OUTLIER_LABEL = -1
OUTLIER_UUID_NAMESPACE = uuid.UUID("00000000-0000-0000-0000-000000000000")


def outlier_uuid(corpus_id: str) -> str:
    """The fixed UUID for ``corpus_id``'s outlier group."""
    return str(uuid.uuid5(OUTLIER_UUID_NAMESPACE, f"outlier:{corpus_id}"))


def jaccard_similarity(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b)


def cluster_item_sets(item_ids: list[str], labels: list[int]) -> dict[int, set[str]]:
    """Group item ids by their (non-outlier) cluster label."""
    groups: dict[int, set[str]] = {}
    for item_id, label in zip(item_ids, labels, strict=True):
        if label == OUTLIER_LABEL:
            continue
        groups.setdefault(label, set()).add(item_id)
    return groups


def match_uuids(
    old_clusters: dict[str, set[str]],
    new_clusters: dict[int, set[str]],
    *,
    threshold: float,
) -> tuple[dict[int, str], list[dict[str, object]]]:
    """Map each new cluster label to a UUID.

    ``old_clusters`` is ``{old_uuid: item_id_set}`` from the previous refit;
    ``new_clusters`` is ``{new_label: item_id_set}`` from this refit (both
    exclude outliers -- that UUID is fixed, see :func:`outlier_uuid`).

    Returns ``(new_label -> uuid, monitoring_rows)``, where each monitoring
    row is ``{old_uuid, new_uuid, jaccard}`` for auditing.
    """
    best_old_for_new: dict[int, tuple[str | None, float]] = {
        new_label: max(
            (
                (old_uuid, jaccard_similarity(new_items, old_items))
                for old_uuid, old_items in old_clusters.items()
            ),
            key=lambda pair: pair[1],
            default=(None, 0.0),
        )
        for new_label, new_items in new_clusters.items()
    }
    best_new_for_old: dict[str, tuple[int | None, float]] = {
        old_uuid: max(
            (
                (new_label, jaccard_similarity(old_items, new_items))
                for new_label, new_items in new_clusters.items()
            ),
            key=lambda pair: pair[1],
            default=(None, 0.0),
        )
        for old_uuid, old_items in old_clusters.items()
    }

    assignments: dict[int, str] = {}
    monitoring: list[dict[str, object]] = []
    for new_label in new_clusters:
        best_old_uuid, jaccard = best_old_for_new[new_label]
        is_mutual_best = (
            best_old_uuid is not None
            and best_new_for_old[best_old_uuid][0] == new_label
        )
        if is_mutual_best and jaccard >= threshold:
            new_uuid = best_old_uuid
        else:
            new_uuid = str(uuid.uuid4())
        assignments[new_label] = new_uuid
        monitoring.append(
            {"old_uuid": best_old_uuid, "new_uuid": new_uuid, "jaccard": jaccard}
        )

    return assignments, monitoring
