import pandas as pd
import typer
from loguru import logger
from offer_categorization.config import features
from offer_categorization.package_api_model import PreprocessingPipeline


def assign_label(df: pd.DataFrame) -> pd.DataFrame:
    ll = sorted(df.offer_subcategory_id.unique())
    label_mapping = pd.DataFrame(
        {"offer_subcategory_id": ll, "label": list(range(len(ll)))}
    )

    return pd.merge(df, label_mapping, on=["offer_subcategory_id"])


def preprocess(
    input_table_path: str = typer.Option("", help="Path to the input table."),
    output_file_dir: str = typer.Option("", help="Directory to save the output files."),
    config_name: str = typer.Option("default", help="Configuration name."),
) -> None:
    features_description = features[config_name]
    logger.info("Preprocess data...")

    pd.read_parquet(input_table_path).pipe(
        PreprocessingPipeline.prepare_features,
        features_description=features_description,
    ).pipe(assign_label).drop(["offer_id", "item_id"], axis=1).to_parquet(
        f"{output_file_dir}/data_clean.parquet"
    )

    logger.info("Preprocessed data saved to %s", output_file_dir)


if __name__ == "__main__":
    typer.run(preprocess)
