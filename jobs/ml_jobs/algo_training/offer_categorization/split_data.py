import pandas as pd
import typer

RANDOM_STATE = 1
TEST_VS_VAL_RATIO = 0.5


def main(
    clean_table_path: str = typer.Option(""),
    split_data_folder: str = typer.Option(""),
    test_ratio: float = typer.Option(0.2),
) -> None:
    if test_ratio < 0 or test_ratio > 1:
        raise ValueError("Test ratio should be between 0 and 1")

    # Sample train, val, test data
    clean_data = pd.read_parquet(clean_table_path).sample(
        frac=1, random_state=RANDOM_STATE
    )
    test_val_data = clean_data.groupby("offer_subcategory_id").sample(
        frac=test_ratio, random_state=RANDOM_STATE
    )

    # Split the data into training, validation and test sets
    train_data = clean_data.drop(test_val_data.index)
    test_data = test_val_data.groupby("offer_subcategory_id").sample(
        frac=TEST_VS_VAL_RATIO, random_state=RANDOM_STATE
    )
    val_data = test_val_data.drop(test_data.index)

    # Save the data
    train_data.to_parquet(f"{split_data_folder}/train.parquet")
    val_data.to_parquet(f"{split_data_folder}/val.parquet")
    test_data.to_parquet(f"{split_data_folder}/test.parquet")


if __name__ == "__main__":
    typer.run(main)
