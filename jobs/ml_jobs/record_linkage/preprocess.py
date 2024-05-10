from tools.config import ENV_SHORT_NAME, GCP_PROJECT_ID
import pandas as pd
import typer


def preprocess(df):
    df = df.fillna("ukn")
    df["offer_id"] = df["offer_id"].values.astype(int)
    df["offer_name"] = df["offer_name"].str.lower()
    df["offer_description"] = df["offer_description"].str.lower()
    df["linked_id"] = ["NC"] * len(df)
    return df


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
    df_offers_to_link = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.tmp_{env_short_name}.items_to_link`"
    )
    df_offers_to_link_clean = preprocess(df_offers_to_link)
    df_offers_to_link_clean.to_gbq(
        f"tmp_{env_short_name}.items_to_link_clean",
        project_id=gcp_project,
        if_exists="replace",
    )


if __name__ == "__main__":
    typer.run(main)
