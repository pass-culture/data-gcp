

import pandas as pd 
import typer
from tools.config import STORAGE_PATH,GCP_PROJECT_ID,ENV_SHORT_NAME

def build_item_id_from_linkage(df):
    for link_ids in df.linked_id.unique():
        df_tmp=df.query(f"linked_id=='{link_ids}'")
        product_ids=df_tmp.item_id.unique()
        item_ids=[item for item in product_ids if 'movie' in item]
        if len(item_ids)>0: 
            df.loc[df['linked_id'] == link_ids,"new_item_id"] = item_ids[0]
        else:
            df.loc[df['linked_id'] == link_ids,"new_item_id"] = link_ids


def main(
    gcp_project: str = typer.Option(
        GCP_PROJECT_ID,
        help="BigQuery Project in which the offers to link is located",
    ),
    env_short_name: str = typer.Option(
        ENV_SHORT_NAME,
        help="Environnement short name",
    ),
    storage_path: str = typer.Option(
        STORAGE_PATH,
        help="Storage path",
    ),
):
    ####
    # Load preprocessed data 
    df_offers_linked_full=pd.read_csv(f"{storage_path}/offers_linked.csv")
    build_item_id_from_linkage(df_offers_linked_full)
    df_offers_linked_full["offer_id"]=df_offers_linked_full["offer_id"].values.astype(str)

    df_offers_linked_full.to_gbq(f"sandbox_{env_short_name}.linked_offers_v0",
        project_id=gcp_project,
        if_exists="replace")

if __name__ == "__main__":
    main()


