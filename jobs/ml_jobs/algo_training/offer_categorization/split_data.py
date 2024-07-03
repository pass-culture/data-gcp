import typer
import pandas as pd


def main(
    clean_table_path: str = typer.Option(""),
    split_data_folder: str = typer.Option(""),
    test_fraction: float = typer.Option(3),
) -> None:
    data_clean = pd.read_parquet(clean_table_path)

    data_clean.sample(frac=1, replace=True, random_state=1)
    test = data_clean.groupby("offer_subcategory_id").sample(
        frac=int(test_fraction) / 10
    )
    data_clean[~data_clean.isin(test).all(axis=1)].to_parquet(
        f"{split_data_folder}/train.parquet"
    )
    test.to_parquet(f"{split_data_folder}/test.parquet")


if __name__ == "__main__":
    typer.run(main)
