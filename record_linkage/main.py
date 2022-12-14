import numpy as np
import networkx as nx
import recordlinkage
import pandas as pd
import uuid
from tools.config import STORAGE_PATH, SUBCATEGORIES_WITH_PERFORMER


def get_matched_df(data_and_hyperparams_dict):

    df_source = data_and_hyperparams_dict["dataframe_to_link"].copy()
    df_matched_list = []
    for subcat in df_source.offer_subcategoryId.unique():
        print("subcat: ", subcat, " On going ..")
        df_source_tmp = df_source.query(f"offer_subcategoryId=='{subcat}'")
        if len(df_source_tmp) == 0:
            print(f"empty subcat {subcat}")
        else:
            indexer = recordlinkage.Index()
            indexer.full()
            print("len df_source_tmp: ", len(df_source_tmp))
            subset_matches_list = []
            subset_k_division = len(df_source_tmp) // 1000
            subset_divisions = subset_k_division if subset_k_division > 0 else 1
            for df_source_tmp_subset in np.array_split(df_source_tmp, subset_divisions):
                if len(df_source_tmp_subset) == 0:
                    print("subset empty..")
                else:
                    # a subset of record pairs
                    candidate_links = indexer.index(df_source_tmp, df_source_tmp_subset)

                    # Comparison step
                    cpr_cl = recordlinkage.Compare()
                    cpr_cl = _setup_matching(
                        cpr_cl, data_and_hyperparams_dict["features"]
                    )
                    ftrs = cpr_cl.compute(candidate_links, df_source_tmp)

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
            df_matched_list.append(_get_linked_offers(df_source_tmp, matches))

    df_mtd = pd.concat(df_matched_list)
    return df_mtd


def _get_linked_offers(df_source, df_matches):
    df_source_tmp = df_source.copy()
    FG = nx.from_pandas_edgelist(
        df_matches, source="index_1", target="index_2", edge_attr=True
    )
    connected_ids = list(nx.connected_components(FG))
    for clusters in range(nx.number_connected_components(FG)):
        link_id = str(uuid.uuid4())
        for index in connected_ids[clusters]:
            df_source_tmp.at[index, "linked_id"] = str(link_id)
    df_linked_offers = df_source_tmp.query("linked_id !='NC' ")
    return df_linked_offers


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


def main():
    ####
    # Load preprocessed data
    df_offers_to_link_clean = pd.read_csv(f"{STORAGE_PATH}/offers_to_link_clean.csv")

    ####
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

    ####
    # Define hyperparameters for each group of offers to links
    data_and_hyperparams_dict = {
        "performer": {
            "dataframe_to_link": df_to_link_performer,
            "features": {
                "offer_name": {"method": "jarowinkler", "threshold": 0.95},
                "offer_description": {"method": "jarowinkler", "threshold": 0.95},
                "performer": {"method": "exact"},
            },
            "matches_requiere": 2,
        },
        "non_performer": {
            "dataframe_to_link": df_to_link_non_performer,
            "features": {
                "offer_name": {"method": "jarowinkler", "threshold": 0.95},
                "offer_description": {"method": "jarowinkler", "threshold": 0.95},
            },
            "matches_requiere": 1,
        },
    }
    ####
    # Run linkage for each group, then with concat both dataframe to get linkage on full data
    df_offers_matched_list = []
    for grp_smp in data_and_hyperparams_dict.keys():
        df_offers_matched_list.append(
            get_matched_df(data_and_hyperparams_dict[grp_smp])
        )
    df_offers_linked_full = pd.concat(df_offers_matched_list)
    df_offers_linked_full.to_csv(f"{STORAGE_PATH}/offers_linked.csv")


if __name__ == "__main__":
    main()
