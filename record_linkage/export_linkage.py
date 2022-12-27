import pandas as pd
import typer
from tools.config import STORAGE_PATH, GCP_PROJECT_ID, ENV_SHORT_NAME


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
    # Load postprocessed data
    df_offers_linked_export_ready = pd.read_csv(
        f"{storage_path}/offers_linked_export_ready.csv"
    )
    ####
    # Extract columns of interrest
    df_offers_linked_export_ready = df_offers_linked_export_ready[
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
    ####
    # Export to Big Query
    df_offers_linked_export_ready.to_gbq(
        f"sandbox_{env_short_name}.linked_offers_v0",
        project_id=gcp_project,
        if_exists="replace",
    )


if __name__ == "__main__":
    typer.run(main)
