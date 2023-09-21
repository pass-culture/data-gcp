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
    logger.info(f"""items[["item_id"]]: {len(items[["item_id"]])}""")
    logger.info(f"pretrained_embedding_df: {len(pretrained_embedding_df)}")
    item_semantic_encoding = pd.concat(
        [items[["item_id"]], pretrained_embedding_df], axis=1
    )
    logger.info(f"item_semantic_encoding: {len(item_semantic_encoding)}")
    # Prepare categorical encoding
    already_used_df = [pretrained_embedding_df]
    del already_used_df
    logger.info("Prepare categorical encoding...")
    items_tmp = items.drop(pretained_embedding_col, axis=1)
    already_used_df = [items]
    del already_used_df
    item_by_category_encoded_reduced, item_by_macro_cat = prepare_categorical_fields(
        items_tmp, splitting
    )
    
    item_semantic_encoding = item_semantic_encoding.drop_duplicates(
        subset=["item_id"], keep="first"
    )
    item_by_category_encoded_reduced = item_by_category_encoded_reduced.drop_duplicates(
        subset=["item_id"], keep="first"
    )
    item_full_encoding = pd.merge(
        item_semantic_encoding,
        item_by_category_encoded_reduced,
        how="left",
        on=["item_id"],
        validate="one_to_one",
    )
    logger.info(f"""item_full_encoding 0.0: {len(item_full_encoding)}""")
    item_full_encoding[["c0", "c1"]] = item_full_encoding[["c0", "c1"]].fillna(
        item_full_encoding[["c0", "c1"]].mean()
    )

    item_by_macro_cat.to_gbq(
        f"tmp_{ENV_SHORT_NAME}.item_by_macro_cat", if_exists="replace"
    )

    item_full_encoding.to_gbq(
        f"tmp_{ENV_SHORT_NAME}.item_full_encoding", if_exists="replace"
    )
    return


if __name__ == "__main__":
    typer.run(preprocess)
