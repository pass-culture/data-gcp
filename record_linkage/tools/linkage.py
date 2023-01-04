import time
import numpy as np
import networkx as nx
import pandas as pd
import recordlinkage
import uuid
from loguru import logger
from tools.config import SUBSET_MAX_LENGTH
from tools.logging_tools import log_memory_info, log_duration
from multiprocessing import cpu_count
import concurrent
import traceback
from itertools import repeat


def run_linkage(
    data_and_hyperparams_dict,
):
    max_process = cpu_count() - 1
    df_matched_list = []
    df_source = data_and_hyperparams_dict["dataframe_to_link"].copy()
    for subcat in df_source.offer_subcategoryId.unique():
        print("subcat: ", subcat, " On going ..")
        df_source_tmp = df_source.query(f"offer_subcategoryId=='{subcat}'")
        if len(df_source_tmp) == 0:
            print(f"empty subcat {subcat}")
        else:
            indexer = recordlinkage.Index()
            indexer.full()
            subset_k_division = len(df_source_tmp) // max_process
            subset_divisions = subset_k_division if subset_k_division > 0 else 1
            print(f"Starting process... with {max_process} CPUs")
            print("subset_divisions: ", subset_divisions)
            batch_number=max_process if subset_divisions>1 else 1
            with concurrent.futures.ProcessPoolExecutor(max_process) as executor:
                futures = executor.map(
                    process_record_linkage,
                    repeat(indexer),
                    repeat(data_and_hyperparams_dict),
                    repeat(df_source_tmp),
                    repeat(subset_divisions),
                    repeat(batch_number),
                    range(batch_number),
                )
                for future in futures:
                    df_matched_list.append(future)
                time.sleep(1)
                executor.shutdown()
    agg_matched_list = []
    for dfs in df_matched_list:
        agg_matched_list.append(dfs)
    return pd.concat(agg_matched_list)


def process_record_linkage(
    indexer, data_and_hyperparams_dict, df_source_tmp, subset_divisions,max_process,batch_number
):
    try:
        return get_linked_offers(
            indexer,
            data_and_hyperparams_dict,
            df_source_tmp,
            subset_divisions,
            max_process,
            batch_number,
        )
    except Exception as e:
        print(e)
        traceback.print_exc()
        return False


def get_linked_offers(
    indexer, data_and_hyperparams_dict, df_source_tmp, subset_divisions,max_process, batch_number
):
    """
    Split linkage by offer_subcategoryId
    Setup Comparaison for linkage
    Run linkage
    """
    subset_matches_list = []
    if batch_number != (max_process - 1):
        df_source_tmp_subset = df_source_tmp[
            batch_number * subset_divisions : (batch_number + 1) * subset_divisions
        ]
    else:
        df_source_tmp_subset = df_source_tmp[batch_number * subset_divisions :]

    if len(df_source_tmp_subset) == 0:
        print("subset empty..")
    else:
        # a subset of record pairs
        candidate_links = indexer.index(df_source_tmp, df_source_tmp_subset)

        # Comparison step
        cpr_cl = recordlinkage.Compare()
        cpr_cl = _setup_matching(cpr_cl, data_and_hyperparams_dict["features"])
        ftrs = cpr_cl.compute(candidate_links, df_source_tmp)

        # Classification step
        mts = ftrs[ftrs.sum(axis=1) >= data_and_hyperparams_dict["matches_required"]]
        mts = mts.reset_index()
        mts = mts.rename(columns={"level_0": "index_1", "level_1": "index_2"})
        subset_matches_list.append(mts)

    df_matched_list=[]
    if len(subset_matches_list)>0:
        matches = pd.concat(subset_matches_list)
        df_matched_list = _get_linked_offers_from_graph(df_source_tmp, matches)

    return df_matched_list


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
