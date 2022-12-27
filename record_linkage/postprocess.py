import pandas as pd
import typer
from tools.config import STORAGE_PATH, GCP_PROJECT_ID, ENV_SHORT_NAME


def build_item_id_from_linkage(df):
    for link_id in df.linked_id.unique():
        df_tmp = df.query(f"linked_id=='{link_id}'")
        product_ids = df_tmp.item_id.unique()
        item_ids = [item for item in product_ids if "movie" in item]
        if len(item_ids) > 0:
            df.loc[df["linked_id"] == link_id, "new_item_id"] = item_ids[0]
        else:
            df.loc[df["linked_id"] == link_id, "new_item_id"] = f"link-{link_id}"


def main(
    storage_path: str = typer.Option(
        STORAGE_PATH,
        help="Storage path",
    ),
):
    ####
    # Load linked offers
    df_offers_linked_full = pd.read_csv(f"{storage_path}/offers_linked.csv")

    ####
    # Build new item_id from linkage
    # If pre-existent item (ex: movie-ABC) is in cluster
    # then all offers in cluster get this item_id instead
    build_item_id_from_linkage(df_offers_linked_full)
    ####
    # Convert offer_id back to string to be consititent with dataset
    df_offers_linked_full["offer_id"] = df_offers_linked_full["offer_id"].values.astype(
        str
    )
    ####
    # Save linked offers with new item_id to be export to Big Query
    df_offers_linked_full.to_csv(f"{storage_path}/offers_linked_export_ready.csv")


if __name__ == "__main__":
    typer.run(main)
