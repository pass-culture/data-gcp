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
    # Load preprocessed data
    df_offers_linked_full = pd.read_csv(f"{storage_path}/offers_linked.csv")
    build_item_id_from_linkage(df_offers_linked_full)
    df_offers_linked_full["offer_id"] = df_offers_linked_full["offer_id"].values.astype(
        str
    )
    df_offers_linked_full.to_csv(f"{storage_path}/offers_linked_export_ready.csv")


if __name__ == "__main__":
    typer.run(main)
