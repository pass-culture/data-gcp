import typer
import pandas as pd

from loguru import logger
from tools.preprocessing import (
    prepare_embedding,
    get_item_by_categories,
    get_item_by_group,
)
from tools.utils import load_config_file, TMP_DATASET


def preprocess(
    input_table: str = typer.Option(..., help="Path to data"),
    output_table: str = typer.Option(..., help="Path to data"),
    config_file_name: str = typer.Option(
        "default-config",
        help="Config file name",
    ),
):
    params = load_config_file(config_file_name)

    logger.info("Loading data: fetch items with metadata and pretained embedding")
    items = pd.read_gbq(f"SELECT * from `{TMP_DATASET}.{input_table}`")

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
    item_embedding_w_group.to_gbq(f"{TMP_DATASET}.{output_table}", if_exists="replace")

    return


if __name__ == "__main__":
    typer.run(preprocess)
