from tools.config import ENV_SHORT_NAME, GCP_PROJECT_ID, MAX_OFFER_PER_BATCH
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
    AND cast(ado.offer_id as STRING) not in (SELECT cast(offer_id as STRING) from `{gcp_project}.analytics_{env_short_name}.offers_already_linked`)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id) = 1
    LIMIT {MAX_OFFER_PER_BATCH}
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
) -> None:
    offers_to_link = get_offers_to_link(gcp_project, env_short_name)
    offers_to_link.to_gbq(
        f"sandbox_{env_short_name}.offers_to_link",
        project_id=gcp_project,
        if_exists="replace",
    )


if __name__ == "__main__":
    typer.run(main)
