import time
import numpy as np
import networkx as nx
import pandas as pd
import recordlinkage
import uuid
from loguru import logger
from tools.config import SUBSET_MAX_LENGTH
from tools.logging_tools import log_memory_info, log_duration


def get_linked_offers(data_and_hyperparams_dict):
    """
    Split linkage by offer_subcategoryId
    Setup Comparaison for linkage
    Run linkage
    """
    start = time.time()
    df_source = data_and_hyperparams_dict["dataframe_to_link"].copy()
    df_matched_list = []
    for subcat in df_source.offer_subcategoryId.unique():
        print("subcat: ", subcat, " On going ..")
        logger.info(log_memory_info())
        df_source_tmp = df_source.query(f"offer_subcategoryId=='{subcat}'")
        if len(df_source_tmp) > 0:
            indexer = recordlinkage.Index()
            indexer.full()
            subset_matches_list = []
            subset_k_division = len(df_source_tmp) // SUBSET_MAX_LENGTH
            subset_divisions = subset_k_division if subset_k_division > 0 else 1
            for df_source_tmp_subset in np.array_split(df_source_tmp, subset_divisions):
                if len(df_source_tmp_subset) > 0:
                    print("Subset on going...")
                    logger.info(log_memory_info())
                    # a subset of record pairs
                    candidate_links = indexer.index(df_source_tmp, df_source_tmp_subset)

                    # Comparison step
                    cpr_cl = recordlinkage.Compare()
                    cpr_cl = _setup_matching(
                        cpr_cl, data_and_hyperparams_dict["features"]
                    )
                    ftrs = cpr_cl.compute(
                        candidate_links, df_source_tmp, df_source_tmp_subset
                    )

                    # Classification step
                    mts = ftrs[
                        ftrs.sum(axis=1)
                        >= data_and_hyperparams_dict["matches_requiere"]
                    ]
                    mts = mts.reset_index()
                    mts = mts.rename(
                        columns={"level_0": "index_1", "level_1": "index_2"}
                    )
                    subset_matches_list.append(mts)

            matches = pd.concat(subset_matches_list)
            df_matched_list.append(
                _get_linked_offers_from_graph(df_source_tmp, matches)
            )

    df_mtd = pd.concat(df_matched_list)
    log_duration(f"get_linked_offers: ", start)
    return df_mtd


def _get_linked_offers_from_graph(df_source, df_matches):
    start = time.time()
    df_source_tmp = df_source.copy()
    FG = nx.from_pandas_edgelist(
        df_matches, source="index_1", target="index_2", edge_attr=True
    )
    connected_ids = list(nx.connected_components(FG))
    for clusters in range(nx.number_connected_components(FG)):
        link_id = str(uuid.uuid4())
        for index in connected_ids[clusters]:
            df_source_tmp.at[index, "linked_id"] = str(link_id)
    df_linked_offers_from_graph = df_source_tmp.query("linked_id !='NC' ")
    df_linked_offers_from_graph["offer_id"] = df_linked_offers_from_graph[
        "offer_id"
    ].values.astype(int)
    log_duration(f"_get_linked_offers_from_graph: ", start)
    return df_linked_offers_from_graph


def _setup_matching(c_cl, featdict):
    for feature in featdict.keys():
        feature_dict = featdict[feature]
        if feature_dict["method"] == "exact":
            c_cl.exact(feature, feature, label=feature)
        else:
            method = feature_dict["method"]
            threshold = feature_dict["threshold"]
            c_cl.string(
                feature, feature, method=method, threshold=threshold, label=feature
            )
    return c_cl
