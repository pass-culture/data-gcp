import pandas as pd

from src.constants import (
    DESCRIPTION_SIMILARITY_COL,
    EVENT_ID_COL,
    IMAGE_SIMILARITY_COL,
    NAME_SIMILARITY_COL,
    OFFER_ID_COL,
)
from src.interfaces import ActionType, CommentType

SCORE_COL = "score"
NEW_OFFER_ID_COL = "new_offer_id"
MATCH_COUNT_COL = "match_count"
MAX_SCORE_COL = "max_score"


def _edge_score(matched_pairs_df: pd.DataFrame) -> pd.Series:
    return pd.concat(
        [
            matched_pairs_df[NAME_SIMILARITY_COL],
            matched_pairs_df[DESCRIPTION_SIMILARITY_COL],
            matched_pairs_df[IMAGE_SIMILARITY_COL] * 100,
        ],
        axis=1,
    ).max(axis=1)


def match_offers_to_existing_events(
    matched_pairs_df: pd.DataFrame, event_offer_link_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Link new offers to already existing event_series based on their matching
        edges to offers already linked to an event_series.

    Existing event_series are frozen: an edge between two already-linked offers
    is ignored (no re-linking, no merging), and an edge between two new offers
    is left untouched for a later clustering pass.

    When a new offer matches offers from several event_series, it is linked to
    the event_series with the most matching offers (majority vote), tie-broken
    by the highest edge score, then by event_id for determinism.

    Args:
        matched_pairs_df (pd.DataFrame): Offer pairs that matched (per
            `clustering.compute_matched_pairs`), with columns "offer_id_1",
            "offer_id_2", and the similarity columns.
        event_offer_link_df (pd.DataFrame): Existing links between offers and
            event_series, with columns "event_id" and "offer_id".

    Returns:
        pd.DataFrame: One row per new offer that could be matched, with
            columns "event_id", "offer_id", "action", "comment".
    """
    linked_offer_ids = set(event_offer_link_df[OFFER_ID_COL])
    offer_to_event = event_offer_link_df.set_index(OFFER_ID_COL)[EVENT_ID_COL].to_dict()

    scored_df = matched_pairs_df.assign(**{SCORE_COL: _edge_score(matched_pairs_df)})

    candidates = []
    for new_suffix, linked_suffix in (("_1", "_2"), ("_2", "_1")):
        new_col = f"{OFFER_ID_COL}{new_suffix}"
        linked_col = f"{OFFER_ID_COL}{linked_suffix}"
        candidates.append(
            scored_df.loc[
                lambda df, n=new_col, linked=linked_col: ~df[n].isin(linked_offer_ids)
                & df[linked].isin(linked_offer_ids)
            ].assign(
                **{
                    NEW_OFFER_ID_COL: lambda df, n=new_col: df[n],
                    EVENT_ID_COL: lambda df, linked=linked_col: df[linked].map(
                        offer_to_event
                    ),
                }
            )
        )

    all_candidates_df = pd.concat(candidates, ignore_index=True)
    if len(all_candidates_df) == 0:
        return pd.DataFrame(columns=[EVENT_ID_COL, OFFER_ID_COL, "action", "comment"])

    aggregated_df = (
        all_candidates_df.groupby([NEW_OFFER_ID_COL, EVENT_ID_COL])[SCORE_COL]
        .agg(**{MATCH_COUNT_COL: "size", MAX_SCORE_COL: "max"})
        .reset_index()
    )
    best_matches_df = aggregated_df.sort_values(
        by=[NEW_OFFER_ID_COL, MATCH_COUNT_COL, MAX_SCORE_COL, EVENT_ID_COL],
        ascending=[True, False, False, True],
    ).drop_duplicates(subset=[NEW_OFFER_ID_COL], keep="first")

    return (
        best_matches_df.rename(columns={NEW_OFFER_ID_COL: OFFER_ID_COL})
        .assign(
            action=ActionType.ADD,
            comment=CommentType.LINKED_TO_EXISTING_EVENT,
        )
        .loc[:, [EVENT_ID_COL, OFFER_ID_COL, "action", "comment"]]
    )


def build_removed_events(
    event_offer_link_df: pd.DataFrame,
    active_offer_ids: set[str],
    comment: CommentType = CommentType.REMOVED_EVENT,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Identify event_series whose offers have all been deleted, and build delta
        records to remove them.

    An offer is considered deleted if it is linked to an event_series but is
    absent from the current set of active offers. An event_series is only
    removed if ALL of its linked offers are deleted; if at least one offer
    survives, nothing is done. Passing an empty `active_offer_ids` therefore
    removes every event_series in `event_offer_link_df`.

    Args:
        event_offer_link_df (pd.DataFrame): Existing links between offers and
            event_series, with columns "event_id" and "offer_id".
        active_offer_ids (set[str]): The set of offer IDs currently active
            (present in the pipeline's raw offer input).
        comment (CommentType): Comment to stamp on the removed events/links.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: A tuple of
            - removed_events_df: one row per removed event_series, with
              columns "event_id", "action", "comment".
            - removed_links_df: one row per removed (event_id, offer_id)
              link, with columns "event_id", "offer_id", "action", "comment".
    """
    deleted_df = event_offer_link_df.assign(
        _deleted=lambda df: ~df[OFFER_ID_COL].isin(active_offer_ids)
    )
    fully_deleted_event_ids = (
        deleted_df.groupby(EVENT_ID_COL)["_deleted"].all().loc[lambda s: s].index
    )

    removed_links_df = (
        deleted_df.loc[
            lambda df: df[EVENT_ID_COL].isin(fully_deleted_event_ids),
            [EVENT_ID_COL, OFFER_ID_COL],
        ]
        .assign(action=ActionType.REMOVE, comment=comment)
        .reset_index(drop=True)
    )
    removed_events_df = pd.DataFrame({EVENT_ID_COL: fully_deleted_event_ids}).assign(
        action=ActionType.REMOVE, comment=comment
    )

    return removed_events_df, removed_links_df
