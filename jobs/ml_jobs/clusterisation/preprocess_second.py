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
import numpy as np


def preprocess(
    splitting: float = typer.Option(
        ..., help="Split proportion between fit and predict"
    ),
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
    item_by_macro_cat = pd.read_gbq(
        f"SELECT * from `tmp_{ENV_SHORT_NAME}.item_by_macro_cat`"
    )

    item_full_encoding = pd.read_gbq(
        f"SELECT * from `tmp_{ENV_SHORT_NAME}.item_full_encoding`"
    )
    # Prepare clusterisation
    logger.info("Prepare clusterisation...")
    item_full_encoding_w_cat_group = prepare_clusterisation(
        item_full_encoding, item_by_macro_cat
    )
    # already_used_df = [
    #     item_semantic_encoding,
    #     item_by_category_encoded_reduced,
    #     item_by_category,
    # ]
    # del already_used_df

    # item_full_encoding_enriched = pd.merge(
    #     items[
    #         [
    #             "item_id",
    #             "rank",
    #             "category",
    #             "subcategory_id",
    #             "offer_sub_type_label",
    #             "offer_type_label",
    #         ]
    #     ],
    #     item_full_encoding_w_cat_group,
    #     how="inner",
    #     on=["item_id"],
    # )
    # already_used_df = [item_full_encoding_w_cat_group]
    # del already_used_df

    item_full_encoding_w_cat_group = item_full_encoding_w_cat_group.astype(str)

    item_full_encoding_w_cat_group.to_gbq(
        f"tmp_{ENV_SHORT_NAME}.item_full_encoding_w_cat_group", if_exists="replace"
    )

    return


if __name__ == "__main__":
    typer.run(preprocess)
