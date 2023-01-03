import pandas as pd
import typer
from tools.config import ENV_SHORT_NAME, GCP_PROJECT_ID, SUBCATEGORIES_WITH_PERFORMER
from tools.linkage import run_linkage


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
    ###############
    # Load preprocessed data
    df_offers_to_link_clean = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.sandbox_{env_short_name}.offers_to_link_clean`"
    )
    ###############
    # Split offers between performer and non performer
    subcat_all = df_offers_to_link_clean.offer_subcategoryId.drop_duplicates().to_list()
    subcat_wo_performer = [
        x for x in subcat_all if x not in SUBCATEGORIES_WITH_PERFORMER
    ]
    df_to_link_performer = df_offers_to_link_clean.query(
        f"""offer_subcategoryId in {tuple(SUBCATEGORIES_WITH_PERFORMER)} """
    )
    df_to_link_non_performer = df_offers_to_link_clean.query(
        f"""offer_subcategoryId in {tuple(subcat_wo_performer)} """
    )

    ###############
    # Define hyperparameters for each group of offers to links
    data_and_hyperparams_dict = {
        "performer": {
            "dataframe_to_link": df_to_link_performer,
            "features": {
                "offer_name": {"method": "jarowinkler", "threshold": 0.95},
                "offer_description": {"method": "jarowinkler", "threshold": 0.95},
                "performer": {"method": "exact"},
            },
            "matches_required": 2,
        },
        "non_performer": {
            "dataframe_to_link": df_to_link_non_performer,
            "features": {
                "offer_name": {"method": "jarowinkler", "threshold": 0.95},
                "offer_description": {"method": "jarowinkler", "threshold": 0.95},
            },
            "matches_required": 1,
        },
    }

    ###############
    # Run linkage for each group (performer, non-performer) then concat both dataframe to get linkage on full data
    df_offers_matched_list = []
    for group_sample in data_and_hyperparams_dict.keys():
        df_offers_matched_list.append(
            run_linkage(data_and_hyperparams_dict[group_sample])
        )
    df_offers_linked_full = pd.concat(df_offers_matched_list)

    df_offers_linked_full.to_gbq(
        f"sandbox_{env_short_name}.linked_offers_full",
        project_id=gcp_project,
        if_exists="replace",
    )
    # Save already linked offers
    df_offers_to_link_clean.to_gbq(
        f"analytics_{env_short_name}.offers_already_linked",
        project_id=gcp_project,
        if_exists="append",
    )


if __name__ == "__main__":
    typer.run(main)
