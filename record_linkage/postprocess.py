import pandas as pd
import typer
from tools.config import GCP_PROJECT_ID, ENV_SHORT_NAME


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
    df_offers_linked_full = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.sandbox_{env_short_name}.linked_offers_full`"
    )
    ####
    # Build new item_id from linkage
    # If pre-existent item (ex: movie-ABC) is in cluster
    # then all offers in cluster get this item_id
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
            "new_item_id",
        ]
    ]
    df_offers_linked_export_ready.to_gbq(
        f"sandbox_{env_short_name}.linked_offers",
        project_id={gcp_project},
        if_exists="replace",
    )


if __name__ == "__main__":
    typer.run(main)
