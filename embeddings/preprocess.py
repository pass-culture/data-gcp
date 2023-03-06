from tools.config import ENV_SHORT_NAME, GCP_PROJECT_ID
import pandas as pd
import typer


def preprocess(df):
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

    df_offers = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.sandbox_{env_short_name}.offers_to_extract_embeddings`"
    )
    df_offers_clean = preprocess(df_offers)
    df_offers_clean.to_gbq(
        f"sandbox_{env_short_name}.offers_to_extract_embeddings_clean",
        project_id=gcp_project,
        if_exists="replace",
    )


if __name__ == "__main__":
    typer.run(main)
