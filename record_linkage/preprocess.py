from tools.config import STORAGE_PATH
import pandas as pd
import typer


def preprocess(df):
    df["offer_id"] = df["offer_id"].values.astype(int)
    df["offer_name"] = df["offer_name"].str.lower()
    df["offer_description"] = df["offer_description"].str.lower()
    df["linked_id"] = ["NC"] * len(df)
    return df


def main(
    storage_path: str = typer.Option(
        STORAGE_PATH,
        help="Storage path",
    )
) -> None:
    #df_offers_to_link = pd.read_csv(f"{storage_path}/offers_to_link.csv")
    df_offers_to_link =pd.read_gbq("SELECT * FROM `passculture-data-prod.sandbox_prod.offers_to_link`")
    df_offers_to_link_clean = preprocess(df_offers_to_link)
    #df_offers_to_link_clean.to_csv(f"{storage_path}/offers_to_link_clean.csv")
    df_offers_to_link_clean.to_gbq(
        f"sandbox_prod.offers_to_link_clean",
        project_id='passculture-data-prod',
        if_exists="replace",
    )

if __name__ == "__main__":
    typer.run(main)
