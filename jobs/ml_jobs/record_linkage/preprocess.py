import pandas as pd
import typer
from tools.config import GCP_PROJECT_ID


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
    input_dataset_name: str = typer.Option(..., help="Path to the dataset input name."),
    input_table_name: str = typer.Option(..., help="Path to the intput table name."),
    output_dataset_name: str = typer.Option(..., help="Path to the dataset name."),
    output_table_name: str = typer.Option(..., help="Path to the output table name."),
) -> None:
    df_offers_to_link = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.{input_dataset_name}.{input_table_name}`"
    )
    df_offers_to_link_clean = preprocess(df_offers_to_link)
    df_offers_to_link_clean.to_gbq(
        f"{output_dataset_name}.{output_table_name}",
        project_id=gcp_project,
        if_exists="replace",
    )


if __name__ == "__main__":
    typer.run(main)
