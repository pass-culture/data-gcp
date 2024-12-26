import networkx as nx
import pandas as pd
import typer
from loguru import logger

from utils.gcs_utils import upload_parquet


def build_graph_and_assign_ids(linked_offers, df_matches):
    G = nx.Graph()
    # Step 2: Add edges
    edges = df_matches[["item_id_synchro", "item_id_candidate"]].values.tolist()
    G.add_edges_from(edges)

    # Add any offer_ids that might not be in the matches as isolated nodes
    all_offer_ids = set(linked_offers["item_id_synchro"])
    matched_offer_ids = set(df_matches["item_id_synchro"]).union(
        df_matches["item_id_candidate"]
    )
    unmatched_offer_ids = all_offer_ids - matched_offer_ids
    G.add_nodes_from(unmatched_offer_ids)

    # Step 3: Find connected components
    connected_components = list(nx.connected_components(G))

    # Step 4: Assign new unique offer IDs
    offer_id_mapping = {}
    for idx, component in enumerate(connected_components):
        new_offer_id = f"new_item_{idx}"
        for offer_id in component:
            offer_id_mapping[offer_id] = new_offer_id

    # Step 5: Map back to the original dataframe
    linked_offers["new_item_id"] = linked_offers["item_id_synchro"].map(
        offer_id_mapping
    )
    return linked_offers


def post_process_graph_matching(linked_offers: pd.DataFrame) -> pd.DataFrame:
    linked_offers_w_id = linked_offers[
        [
            "item_id_synchro",
            "item_id_candidate",
            "offer_subcategory_id_synchro",
            "new_item_id",
        ]
    ].rename(columns={"offer_subcategory_id_synchro": "offer_subcategory_id"})

    linked_offers_w_id_clean = linked_offers_w_id[
        linked_offers_w_id["item_id_synchro"] != linked_offers_w_id["item_id_candidate"]
    ]

    df_1 = (
        linked_offers_w_id_clean[["item_id_synchro", "new_item_id"]]
        .rename(columns={"item_id_synchro": "item_id"})
        .drop_duplicates()
    )
    df_2 = (
        linked_offers_w_id_clean[["item_id_candidate", "new_item_id"]]
        .rename(columns={"item_id_candidate": "item_id"})
        .drop_duplicates()
    )

    linked_offers_w_id_clean = pd.concat([df_1, df_2])
    return linked_offers_w_id_clean


def assign_linked_ids(
    input_path: str = typer.Option(default=..., help="GCS parquet path"),
    output_path: str = typer.Option(default=..., help="GCS parquet output path"),
) -> None:
    linked_offers = pd.read_parquet(input_path)
    df_matches = linked_offers[["item_id_synchro", "item_id_candidate"]]
    # Step 1: Build the graph
    linked_offers_w_id = build_graph_and_assign_ids(linked_offers, df_matches)

    # Display the updated dataframe
    # print(df_offers)

    linked_offers_w_id_clean = post_process_graph_matching(linked_offers_w_id)
    logger.info("Uploading linkage output..")
    upload_parquet(
        dataframe=linked_offers_w_id_clean,
        gcs_path=output_path,
    )
    return
