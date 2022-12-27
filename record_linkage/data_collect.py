from tools.config import STORAGE_PATH, ENV_SHORT_NAME, GCP_PROJECT_ID
import pandas as pd
import typer


def get_offers_to_link(gcp_project, env_short_name):
    """
    Extract offers to link via record linkage. Only ONE offer by item
    NB: Books are excluded
    """
    query = f"""
    SELECT
    ado.offer_id,
    oii.item_id,
    ado.offer_subcategoryId,
    ado.offer_name,
    ado.offer_description,
    oed.performer
    FROM `{gcp_project}.analytics_{env_short_name}.applicative_database_offer` ado
    LEFT JOIN `{gcp_project}.analytics_{env_short_name}.offer_item_ids` oii on oii.offer_id = ado.offer_id 
    LEFT JOIN `{gcp_project}.analytics_{env_short_name}.offer_extracted_data` oed on oed.offer_id = ado.offer_id 
    WHERE ado.offer_subcategoryId != 'LIVRE_PAPIER'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id) = 1
    LIMIT 100000
    """
    return pd.read_gbq(query)


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
) -> None:
    offers_to_link = get_offers_to_link(gcp_project, env_short_name)
    offers_to_link.to_csv(f"{storage_path}/offers_to_link.csv", index=False)


if __name__ == "__main__":
    typer.run(main)
