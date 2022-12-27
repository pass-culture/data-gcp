import pandas as pd
import typer
from tools.config import STORAGE_PATH, SUBCATEGORIES_WITH_PERFORMER, SUBSET_MAX_LENGTH
from tools.linkage import get_linked_offers


def main(
    storage_path: str = typer.Option(
        STORAGE_PATH,
        help="Storage path",
    )
) -> None:
    ###############
    # Load preprocessed data
    #df_offers_to_link_clean = pd.read_csv(f"{storage_path}/offers_to_link_clean.csv")
    df_offers_to_link_clean=pd.read_gbq("SELECT * FROM `passculture-data-prod.sandbox_prod.offers_to_link_clean`")
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
            "matches_requiere": 2,
        },
        "non_performer": {
            "dataframe_to_link": df_to_link_non_performer,
            "features": {
                "offer_name": {"method": "jarowinkler", "threshold": 0.95},
                "offer_description": {"method": "jarowinkler", "threshold": 0.95},
            },
            "matches_requiere": 1,
        },
    }

    ###############
    # Run linkage for each group (performer, non-performer) then concat both dataframe to get linkage on full data
    df_offers_matched_list = []
    for group_sample in data_and_hyperparams_dict.keys():
        df_offers_matched_list.append(
            get_linked_offers(data_and_hyperparams_dict[group_sample])
        )
    df_offers_linked_full = pd.concat(df_offers_matched_list)
    #df_offers_linked_full.to_csv(f"{storage_path}/offers_linked.csv")
    df_offers_linked_full.to_gbq(
        f"sandbox_prod.linked_offers_full",
        project_id='passculture-data-prod',
        if_exists="replace",
    )

if __name__ == "__main__":
    typer.run(main)
