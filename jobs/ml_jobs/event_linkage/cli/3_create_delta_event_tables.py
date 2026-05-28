import pandas as pd
import typer
from loguru import logger
from tqdm import tqdm

from src.clustering import (
    clusterize_offers,
    extract_cluster_metadata,
    get_uuid_from_cluster,
)
from src.constants import (
    IMAGE_URL_COL,
    OFFER_ID_COL,
    OFFER_SUBCATEGORY_ID_COL,
)
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
    delta_events_filepath: str = typer.Option(),
    delta_event_offer_links_filepath: str = typer.Option(),
) -> None:
    # 1. Load Data
    raw_data_df = pd.read_parquet(offer_event_filepath)
    similarities_df = pd.read_parquet(similarities_filepath)

    # 2. Build cross df with similarities and raw data
    cross_df = build_cross_df(raw_data_df, similarities_df)

    # 3. Clusterize per subcategory
    logger.info("Clusterizing offers into events...")
    cluster_dfs = []
    for subcategory in raw_data_df[OFFER_SUBCATEGORY_ID_COL].dropna().unique():
        logger.info(f"Clusterizing subcategory {subcategory}...")
        cluster_df = clusterize_offers(cross_df, subcategory)
        logger.success(
            f"Subcategory {subcategory} clusterized into {len(cluster_df)} clusters "
            f"with {cluster_df['cluster_length'].sum()} offers in clusters."
        )
        cluster_dfs.append(cluster_df)

    all_cluster_df = pd.concat(cluster_dfs, ignore_index=True)
    logger.success(
        f"Clusterized offers into {len(all_cluster_df)} clusters across "
        f"{len(cluster_dfs)} subcategories."
    )

    # 4. Create Delta Events
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

    pd.DataFrame(delta_events).to_parquet(delta_events_filepath, index=False)
    pd.DataFrame(delta_event_offer_links).explode("offer_ids").rename(
        columns={"offer_ids": OFFER_ID_COL}
    ).to_parquet(delta_event_offer_links_filepath, index=False)
    logger.success(
        f"Saved delta events to {delta_events_filepath} and "
        f"delta event-offer links to {delta_event_offer_links_filepath}."
    )


if __name__ == "__main__":
    app()
