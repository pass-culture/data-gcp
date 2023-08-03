import typer
import pandas as pd
import json
from loguru import logger
from tools.preprocessing import (
    prepare_pretrained_encoding,
    prepare_categorical_fields,
    prepare_clusterisation,
)
from tools.utils import ENV_SHORT_NAME, CONFIGS_PATH


def preprocess(
    input_table: str = typer.Option(..., help="Path to data"),
    output_table: str = typer.Option(..., help="Path to data"),
    config_file_name: str = typer.Option(
        "default-config-offer",
        help="Config file name",
    ),
):
    with open(
        f"{CONFIGS_PATH}/{config_file_name}.json",
        mode="r",
        encoding="utf-8",
    ) as config_file:
        params = json.load(config_file)

    logger.info("Loading data: fetch item with pretained encoding")
    items = pd.read_gbq(f"SELECT * from `tmp_{ENV_SHORT_NAME}.{input_table}`")

    # Prepare semantic encoding
    logger.info("Prepare semantice encoding...")
    pretained_embedding_col = [
        col
        for col in items.columns.tolist()
        if params["pretained_embedding_name"] in col
    ][0]

    pretrained_embedding_df = prepare_pretrained_encoding(
        items[pretained_embedding_col].tolist()
    )

    item_semantic_encoding = pd.concat(
        [items[["item_id"]], pretrained_embedding_df], axis=1
    )

    # Prepare categorical encoding
    logger.info("Prepare categorical encoding...")
    items_tmp = items.drop(pretained_embedding_col, axis=1)
    item_by_category_encoded_reduced, item_by_category = prepare_categorical_fields(
        items_tmp
    )

    # Prepare clusterisation
    logger.info("Prepare clusterisation...")
    item_full_encoding_w_cat_group = prepare_clusterisation(
        item_semantic_encoding, item_by_category_encoded_reduced, item_by_category
    )

    item_full_encoding_enriched = pd.concat(
        [
            items[
                [
                    "category",
                    "subcategory_id",
                    "offer_sub_type_label",
                    "offer_type_label",
                ]
            ],
            item_full_encoding_w_cat_group,
        ],
        axis=1,
    )
    item_full_encoding_enriched = item_full_encoding_enriched.astype(str)

    item_full_encoding_enriched.to_gbq(
        f"tmp_{ENV_SHORT_NAME}.{output_table}", if_exists="replace"
    )

    return


if __name__ == "__main__":
    typer.run(preprocess)
