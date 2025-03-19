import networkx as nx
import pandas as pd
import typer
from loguru import logger

from utils.common import read_parquet_files_from_gcs_directory
from utils.gcs_utils import upload_parquet

app = typer.Typer()


def build_graph_and_assign_ids(linked_offers: pd.DataFrame) -> pd.DataFrame:
    G = nx.Graph()
    edges = linked_offers[["item_id_synchro", "item_id_candidate"]].values.tolist()
    G.add_edges_from(edges)

    connected_components = list(nx.connected_components(G))

    offer_id_mapping = {}
    for idx, component in enumerate(connected_components):
        new_offer_id = f"item_cluster_{idx}"
        for offer_id in component:
            offer_id_mapping[offer_id] = new_offer_id

    linked_offers["new_item_id"] = linked_offers["item_id_synchro"].map(
        offer_id_mapping
    )
    counts = linked_offers.groupby("new_item_id")["new_item_id"].transform("size")
    linked_offers = linked_offers[counts > 1]
    return linked_offers


def post_process_graph_matching(linked_offers_w_id: pd.DataFrame) -> pd.DataFrame:
    linked_offers_w_id_clean = linked_offers_w_id[
        linked_offers_w_id["item_id_synchro"] != linked_offers_w_id["item_id_candidate"]
    ]

    df_left = linked_offers_w_id_clean[["item_id_synchro", "new_item_id"]].rename(
        columns={"item_id_synchro": "item_id"}
    )
    df_right = linked_offers_w_id_clean[["item_id_candidate", "new_item_id"]].rename(
        columns={"item_id_candidate": "item_id"}
    )
    linked_offers_w_id_clean = pd.concat([df_left, df_right]).drop_duplicates()
    linked_offers_w_id_clean = linked_offers_w_id_clean.rename(
        columns={"item_id": "item_id_candidate"}
    )
    return linked_offers_w_id_clean


@app.command()
def main(
    input_path: str = typer.Option(default=..., help="GCS parquet path"),
    output_path: str = typer.Option(default=..., help="GCS parquet output path"),
) -> None:
    """
    Build a graph, assign IDs, and post-process the results.

    This function:
      1) Builds a graph and assigns IDs to the linked offers.
      3) Post-processes the graph matching results.
      4) Saves the cleaned results to the specified output path.

    Args:
        input_path (str): The GCS path to the input parquet file containing linked offers.
        output_path (str): The GCS path to save the output parquet file with processed results.
    """
    linked_offers = read_parquet_files_from_gcs_directory(input_path)
    logger.info(f"linked_offers: {linked_offers.columns}")

    linked_offers_w_id = build_graph_and_assign_ids(linked_offers)

    linked_offers_w_id_clean = post_process_graph_matching(linked_offers_w_id)
    linked_offers_w_id_enriched = linked_offers_w_id_clean.merge(
        linked_offers[
            [col for col in linked_offers.columns if col.endswith("candidate")]
        ],
        on=["item_id_candidate"],
        how="left",
    ).drop_duplicates()
    logger.info(f"linked_offers_w_id_enriched: {linked_offers_w_id_enriched.columns}")
    logger.info("Uploading linkage output..")
    upload_parquet(
        dataframe=linked_offers_w_id_enriched,
        gcs_path=f"{output_path}/data.parquet",
    )
    return


if __name__ == "__main__":
    app()
