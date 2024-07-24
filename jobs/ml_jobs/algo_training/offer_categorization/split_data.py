import typer
import pandas as pd
from utils.gcs_utils import upload_parquet

app = typer.Typer()


@app.command()
def main(
    clean_table_path: str = typer.Option(""), split_data_folder: str = typer.Option("")
) -> None:
    data_clean = pd.read_parquet(clean_table_path)

    # Your code here
    data_clean.sample(frac=1, replace=True, random_state=1)
    test_frac = 0.3
    test = data_clean.groupby("offer_subcategory_id").sample(frac=test_frac)
    train = data_clean[~data_clean.isin(test).all(axis=1)]

    test.to_parquet(f"{split_data_folder}/test.parquet")
    train.to_parquet(f"{split_data_folder}/train.parquet")
    upload_parquet(
        dataframe=train,
        gcs_path=f"{split_data_folder}/train.parquet",
    )

    upload_parquet(
        dataframe=test,
        gcs_path=f"{split_data_folder}/test.parquet",
    )


if __name__ == "__main__":
    app()
