import time
import uuid

import networkx as nx
import recordlinkage
from tools.logging_tools import log_duration


def get_linked_offers(
    indexer,
    data_and_hyperparams_dict,
    df_source_tmp,
    subset_length,
    batch_number,
    batch_id,
):
    """
    Split linkage by offer_subcategoryId
    Setup Comparaison for linkage
    Run linkage
    """
    if batch_id != (batch_number - 1):
        df_source_tmp_subset = df_source_tmp[
            batch_id * subset_length : (batch_id + 1) * subset_length
        ]
    else:
        df_source_tmp_subset = df_source_tmp[batch_id * subset_length :]

    if len(df_source_tmp_subset) > 0:
        # a subset of record pairs
        candidate_links = indexer.index(df_source_tmp, df_source_tmp_subset)

        # Comparison step
        cpr_cl = recordlinkage.Compare()
        cpr_cl = _setup_matching(cpr_cl, data_and_hyperparams_dict["features"])
        ftrs = cpr_cl.compute(candidate_links, df_source_tmp)

        # Classification step
        matches = ftrs[
            ftrs.sum(axis=1) >= data_and_hyperparams_dict["matches_required"]
        ]
        matches = matches.reset_index()
        matches = matches.rename(columns={"level_0": "index_1", "level_1": "index_2"})
    return matches


def get_linked_offers_from_graph(df_source, df_matches):
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
