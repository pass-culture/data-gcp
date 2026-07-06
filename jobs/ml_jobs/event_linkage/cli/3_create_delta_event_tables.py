import pandas as pd
import typer
from loguru import logger
from tqdm import tqdm

from src.clustering import (
    build_clusters_from_matched_pairs,
    compute_matched_pairs,
    extract_cluster_metadata,
    get_uuid_from_cluster,
)
from src.constants import (
    EVENT_ID_COL,
    EVENT_NAME_COL,
    EVENT_SERIES_ID_COL,
    IMAGE_URL_COL,
    OFFER_ID_COL,
    OFFER_SUBCATEGORY_ID_COL,
)
from src.delta import build_removed_events, match_offers_to_existing_events
from src.interfaces import ActionType, CommentType


def build_cross_df(
    raw_data_df: pd.DataFrame, similarities_df: pd.DataFrame
) -> pd.DataFrame:
    RAW_COLUMNS = [
        OFFER_ID_COL,
        OFFER_SUBCATEGORY_ID_COL,
        IMAGE_URL_COL,
    ]
    selected_df = raw_data_df.loc[:, lambda df: df.columns.isin(RAW_COLUMNS)]

    return similarities_df.merge(
        selected_df.add_suffix("_1"),
        on=f"{OFFER_ID_COL}_1",
        how="left",
    ).merge(
        selected_df.add_suffix("_2"),
        on=f"{OFFER_ID_COL}_2",
        how="left",
    )


app = typer.Typer()


@app.command()
def main(
    offer_event_filepath: str = typer.Option(),
    similarities_filepath: str = typer.Option(),
    event_series_offer_link_filepath: str = typer.Option(),
    delta_events_filepath: str = typer.Option(),
    delta_event_offer_link_filepath: str = typer.Option(),
) -> None:
    # 1. Load Data
    raw_data_df = pd.read_parquet(offer_event_filepath)
    similarities_df = pd.read_parquet(similarities_filepath)
    event_offer_link_df = (
        pd.read_parquet(event_series_offer_link_filepath)
        .rename(columns={EVENT_SERIES_ID_COL: EVENT_ID_COL})
        .astype({OFFER_ID_COL: raw_data_df[OFFER_ID_COL].dtype})
    )
    if (
        len(event_offer_link_df) > 0
        and not event_offer_link_df[OFFER_ID_COL].isin(raw_data_df[OFFER_ID_COL]).any()
    ):
        logger.warning(
            "None of the offers in the event-series-offer-link file are present "
            "in the raw offers input. This usually indicates an offer_id dtype "
            "mismatch between the two files and would cause every existing "
            "event_series to be flagged as removed."
        )

    # 2. Build cross df with similarities and raw data
    cross_df = build_cross_df(raw_data_df, similarities_df)

    # 3. Per subcategory: link new offers to existing events, then clusterize
    #    the remaining unmatched offers into new events
    logger.info("Matching and clusterizing offers per subcategory...")
    existing_linked_offer_ids = set(event_offer_link_df[OFFER_ID_COL])
    cluster_dfs = []
    existing_event_links_dfs = []
    for subcategory in raw_data_df[OFFER_SUBCATEGORY_ID_COL].dropna().unique():
        logger.info(f"Processing subcategory {subcategory}...")
        matched_pairs_df = compute_matched_pairs(cross_df, subcategory)

        existing_event_links_df = match_offers_to_existing_events(
            matched_pairs_df, event_offer_link_df
        )
        existing_event_links_dfs.append(existing_event_links_df)
        newly_linked_offer_ids = set(existing_event_links_df[OFFER_ID_COL])

        excluded_offer_ids = existing_linked_offer_ids | newly_linked_offer_ids
        remaining_pairs_df = matched_pairs_df.loc[
            lambda df, excluded=excluded_offer_ids: ~df[f"{OFFER_ID_COL}_1"].isin(
                excluded
            )
            & ~df[f"{OFFER_ID_COL}_2"].isin(excluded)
        ]
        cluster_df = build_clusters_from_matched_pairs(remaining_pairs_df, subcategory)
        logger.success(
            f"Subcategory {subcategory}: linked {len(newly_linked_offer_ids)} offers "
            f"to existing events, clusterized into {len(cluster_df)} new clusters "
            f"with {cluster_df['cluster_length'].sum()} offers in clusters."
        )
        cluster_dfs.append(cluster_df)

    all_cluster_df = pd.concat(cluster_dfs, ignore_index=True)
    all_existing_event_links_df = pd.concat(existing_event_links_dfs, ignore_index=True)
    logger.success(
        f"Clusterized offers into {len(all_cluster_df)} new clusters and linked "
        f"{len(all_existing_event_links_df)} offers to existing events across "
        f"{len(cluster_dfs)} subcategories."
    )

    # 4. Create Delta Events from new clusters
    logger.info("Creating delta events from clusters...")
    delta_events = []
    delta_event_offer_links = []
    for cluster_row in tqdm(
        all_cluster_df.sort_values(by="cluster_length", ascending=False).itertuples(),
        total=len(all_cluster_df),
    ):
        # Extract cluster metadata to create event
        cluster_metadata = extract_cluster_metadata(cluster_row, cross_df, raw_data_df)
        event_id = get_uuid_from_cluster(cluster_row.cluster)

        if cluster_metadata[EVENT_NAME_COL] is None:
            logger.warning(
                f"Cluster {cluster_row.Index} with offers {cluster_row.cluster} "
                f"has no event name, skipping..."
            )
            continue

        delta_events.append(
            {
                "event_id": event_id,
                **cluster_metadata,
                "action": ActionType.ADD,
                "comment": CommentType.NEW_EVENT,
            }
        )

        delta_event_offer_links.append(
            {
                "event_id": event_id,
                "offer_ids": cluster_row.cluster,
                "action": ActionType.ADD,
                "comment": CommentType.NEW_EVENT,
            }
        )
    logger.success(f"Created {len(delta_events)} delta events from clusters.")

    # 5. Remove events whose offers have all been deleted
    active_offer_ids = set(raw_data_df[OFFER_ID_COL])
    removed_events_df, removed_event_links_df = build_removed_events(
        event_offer_link_df, active_offer_ids
    )
    logger.success(f"Removed {len(removed_events_df)} events with no active offers.")

    # 6. Assemble and save delta tables
    all_delta_events_df = pd.concat(
        [pd.DataFrame(delta_events), removed_events_df], ignore_index=True
    )
    all_delta_event_offer_links_df = pd.concat(
        [
            pd.DataFrame(delta_event_offer_links)
            .explode("offer_ids")
            .rename(columns={"offer_ids": OFFER_ID_COL}),
            all_existing_event_links_df,
            removed_event_links_df,
        ],
        ignore_index=True,
    )

    all_delta_events_df.to_parquet(delta_events_filepath, index=False)
    all_delta_event_offer_links_df.to_parquet(
        delta_event_offer_link_filepath, index=False
    )
    logger.success(
        f"Saved delta events to {delta_events_filepath} and "
        f"delta event-offer links to {delta_event_offer_link_filepath}."
    )


if __name__ == "__main__":
    app()
