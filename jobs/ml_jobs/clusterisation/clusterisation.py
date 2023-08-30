import typer
import pandas as pd
import json
from loguru import logger
from tools.clusterisation_tools import clusterisation_from_prebuild_encoding
from tools.utils import CONFIGS_PATH, ENV_SHORT_NAME


def clusterization(
    input_table: str = typer.Option(..., help="Path to data"),
    output_table: str = typer.Option(..., help="Path to data"),
    config_file_name: str = typer.Option(
        "default-config",
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
    item_full_encoding_enriched = pd.read_gbq(
        f"SELECT * from `tmp_{ENV_SHORT_NAME}.{input_table}`"
    )
    # Perform clusterisation
    logger.info("Perform clusterisation...")
    category_list = (
        item_full_encoding_enriched.groupby(["category_group"])
        .agg(item_id_size=("item_id", "size"))
        .reset_index()["category_group"]
        .values.tolist()
    )
    df_clusters_enriched = pd.DataFrame()
    for cat in category_list:
        item_full_encoding_enriched_given_cat = item_full_encoding_enriched.loc[
            item_full_encoding_enriched["category_group"].isin([cat])
        ]
        dfclusters_cat = clusterisation_from_prebuild_encoding(
            cat,
            item_full_encoding_enriched_given_cat,
            params["target_nbclusters"],
            output_table,
        )
        dfclusters_cat["category"] = [cat] * len(dfclusters_cat)
        dfclusters_cat["cluster_id"] = dfclusters_cat[["cluster", "category"]].agg(
            "|".join, axis=1
        )
        dfclusters_cat["cluster_name"] = dfclusters_cat[["cluster_id", "label"]].agg(
            "|".join, axis=1
        )
        df_clusters_enriched = pd.concat([df_clusters_enriched, dfclusters_cat])
    df_clusters_enriched.to_gbq(
        f"""clean_{ENV_SHORT_NAME}.{output_table}_{params["target_nbclusters"]}_ALL""",
        if_exists="replace",
    )

    return


if __name__ == "__main__":
    typer.run(clusterization)
