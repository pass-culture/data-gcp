import pandas as pd
import typer
from tools.config import ENV_SHORT_NAME, GCP_PROJECT_ID, SUBCATEGORIES_WITH_PERFORMER
from multiprocessing import cpu_count
import concurrent
import traceback
from itertools import repeat
import recordlinkage
import time
import numpy as np
import networkx as nx
import pandas as pd
import uuid
from tools.config import SUBSET_MAX_LENGTH
from tools.logging_tools import log_duration


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


def get_linked_offers(
    indexer,
    data_and_hyperparams_dict,
    df_source_tmp,
    subset_divisions,
    max_process,
    batch_number,
):
    """
    Split linkage by offer_subcategoryId
    Setup Comparaison for linkage
    Run linkage
    """
    if batch_number != (max_process - 1):
        df_source_tmp_subset = df_source_tmp[
            batch_number * subset_divisions : (batch_number + 1) * subset_divisions
        ]
    else:
        df_source_tmp_subset = df_source_tmp[batch_number * subset_divisions :]

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
        df_matched = _get_linked_offers_from_graph(df_source_tmp, matches)

    return df_matched


def process_record_linkage(
    indexer,
    data_and_hyperparams_dict,
    df_source_tmp,
    subset_divisions,
    max_process,
    batch_number,
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


def main(
    gcp_project,
    env_short_name,
) -> None:
    ###############
    # Load preprocessed data
    df_offers_to_link_clean = pd.read_gbq(
        f"SELECT * FROM `{gcp_project}.sandbox_{env_short_name}.offers_to_link_clean`"
    )
    ###############
    # Split offers between performer and non performer
    subcat_all = df_offers_to_link_clean.offer_subcategoryId.drop_duplicates().to_list()
    subcat_wo_performer = [
        x for x in subcat_all if x not in SUBCATEGORIES_WITH_PERFORMER
    ]
    df_to_link_performer = df_offers_to_link_clean.query(
        f"""offer_subcategoryId in {tuple(SUBCATEGORIES_WITH_PERFORMER)} """
    )
    df_to_link_non_performer = df_offers_to_link_clean.query(
        f"""offer_subcategoryId in {tuple(subcat_wo_performer)} """
    )

    ###############
    # Define hyperparameters for each group of offers to links
    data_and_hyperparams_dict = {
        "performer": {
            "dataframe_to_link": df_to_link_performer,
            "features": {
                "offer_name": {"method": "jarowinkler", "threshold": 0.95},
                "offer_description": {"method": "jarowinkler", "threshold": 0.95},
                "performer": {"method": "exact"},
            },
            "matches_required": 2,
        },
        "non_performer": {
            "dataframe_to_link": df_to_link_non_performer,
            "features": {
                "offer_name": {"method": "jarowinkler", "threshold": 0.95},
                "offer_description": {"method": "jarowinkler", "threshold": 0.95},
            },
            "matches_required": 1,
        },
    }

    ###############
    # Run linkage for each group (performer, non-performer) then concat both dataframe to get linkage on full data
    df_offers_matched_list = []
    for group_sample in data_and_hyperparams_dict.keys():
        data_and_hyperparams_dict = data_and_hyperparams_dict[group_sample]
        max_process = cpu_count() - 1
        df_matched_list = []
        df_source = data_and_hyperparams_dict["dataframe_to_link"].copy()
        for subcat in df_source.offer_subcategoryId.unique():
            print("subcat: ", subcat, " On going ..")
            df_source_tmp = df_source.query(f"offer_subcategoryId=='{subcat}'")
            if len(df_source_tmp) > 0:
                indexer = recordlinkage.Index()
                indexer.full()
                subset_k_division = len(df_source_tmp) // max_process
                subset_divisions = subset_k_division if subset_k_division > 0 else 1
                print(f"Starting process... with {max_process} CPUs")
                print("subset_divisions: ", subset_divisions)
                batch_number = max_process if subset_divisions > 1 else 1
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
                time.sleep(60)
        df_offers_matched_list.append(pd.concat(df_matched_list))

    df_offers_linked_full = pd.concat(df_offers_matched_list)

    df_offers_linked_full.to_gbq(
        f"sandbox_{env_short_name}.linked_offers_full",
        project_id=gcp_project,
        if_exists="replace",
    )
    # Save already linked offers
    df_offers_to_link_clean.to_gbq(
        f"analytics_{env_short_name}.offers_already_linked",
        project_id=gcp_project,
        if_exists="append",
    )


if __name__ == "__main__":
    main(GCP_PROJECT_ID, ENV_SHORT_NAME)
