import pandas as pd
import typer
from loguru import logger

from tools.preprocessing import (
    get_item_by_categories,
    get_item_by_group,
    prepare_embedding,
)
from tools.utils import load_config_file


def preprocess(
    input_dataset_name: str = typer.Option(..., help="Input dataset name."),
    input_table_name: str = typer.Option(..., help="Input table name."),
    output_dataset_name: str = typer.Option(..., help="Output dataset name."),
    output_table_name: str = typer.Option(..., help="Ouput table name."),
    config_file_name: str = typer.Option(
        "default-config",
        help="Config file name",
    ),
):
    """
    Preprocesses the data in groups based {config_file_name} for clustering.

    Args:
        input_table (str): Path to the input data.
        output_table (str): Path to the output data.
        config_file_name (str, optional): Config file name. Defaults to "default-config".

    Returns:
        None
    """

    params = load_config_file(config_file_name, job_type="cluster")

    logger.info("Loading data: fetch items with metadata and pretained embedding")
    items: pd.DataFrame = pd.read_gbq(
        f"SELECT * from `{input_dataset_name}.{input_table_name}`"
    )

    logger.info("Build item groups...")
    item_clean = (
        items[
            [
                "item_id",
                "category",
                "subcategory_id",
                "offer_type_label",
                "offer_sub_type_label",
            ]
        ]
        .fillna("")
        .replace("None", "")
    )
    item_by_category = get_item_by_categories(item_clean)
    item_by_group = get_item_by_group(item_by_category, params["group_config"])

    logger.info("Prepare pretrained embedding...")
    embedding_clean = prepare_embedding(
        items[params["pretrained_embedding_name"]].tolist(),
        params["pretrained_embedding_size"],
    )
    item_embedding = pd.concat([items[["item_id"]], embedding_clean], axis=1)
    logger.info(f"item_embedding: {len(item_embedding)}")

    item_embedding_w_group = pd.merge(
        item_embedding, item_by_group, how="inner", on=["item_id"]
    )
    item_embedding_w_group = item_embedding_w_group.fillna(0)
    logger.info(f"item_embedding: {len(item_embedding_w_group)}")
    item_embedding_w_group.to_gbq(
        f"{output_dataset_name}.{output_table_name}", if_exists="replace"
    )

    return


if __name__ == "__main__":
    typer.run(preprocess)
