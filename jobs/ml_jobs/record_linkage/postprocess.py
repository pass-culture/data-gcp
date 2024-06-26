from datetime import datetime

import pandas as pd
import typer
from loguru import logger
from tools.config import ENV_SHORT_NAME, GCP_PROJECT_ID


def build_item_id_from_linkage(df):
    synchro_products = ["movie", "cd", "vinyle"]
    for link_id in df.linked_id.unique():
        df_tmp = df.query(f"linked_id=='{link_id}'")
        df_tmp = df_tmp.dropna()
        product_ids = list(df_tmp.item_id.unique())
        # logger.info(f"product_ids: {print(product_ids)}")
        if product_ids is not None:
            item_ids = [
                item for item in product_ids if any(x in item for x in synchro_products)
            ]
        else:
            item_ids = []
        if len(item_ids) > 0:
            df.loc[df["linked_id"] == link_id, "item_linked_id"] = item_ids[0]
        else:
            df.loc[df["linked_id"] == link_id, "item_linked_id"] = f"link-{link_id}"


def main(
    gcp_project: str = typer.Option(
        GCP_PROJECT_ID,
        help="BigQuery Project in which the offers to link is located",
    ),
    env_short_name: str = typer.Option(
        ENV_SHORT_NAME,
        help="Environnement short name",
    ),
):
    ####
    # Load linked offers
    logger.info("Loading data from linkage to build label for linked items ")
    df_offers_linked_full = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.tmp_{env_short_name}.linked_offers_full`"
    )
    logger.info(f"{len(df_offers_linked_full)} items to label")
    ####
    # Build new item_id from linkage
    # If pre-existent item (ex: movie-ABC) is in cluster
    # then all offers in cluster get this item_id
    logger.info("Building label from linkage data...")
    build_item_id_from_linkage(df_offers_linked_full)
    ####
    # Convert offer_id back to string to be consititent with dataset
    df_offers_linked_full["offer_id"] = df_offers_linked_full["offer_id"].values.astype(
        str
    )
    ####
    # Save linked offers with new item_id to be export to Big Query
    # df_offers_linked_full.to_csv(f"{storage_path}/offers_linked_export_ready.csv")
    df_offers_linked_export_ready = df_offers_linked_full[
        [
            "offer_id",
            "item_id",
            "offer_subcategoryId",
            "offer_name",
            "offer_description",
            "performer",
            "linked_id",
            "item_linked_id",
        ]
    ]
    df_offers_linked_export_ready["extraction_date"] = [
        datetime.now().strftime("%Y-%m-%d")
    ] * len(df_offers_linked_export_ready)
    df_offers_linked_export_ready = df_offers_linked_export_ready.drop_duplicates()
    df_offers_linked_export_ready.to_gbq(
        f"analytics_{env_short_name}.linked_offers",
        project_id=gcp_project,
        if_exists="append",
    )


if __name__ == "__main__":
    typer.run(main)
