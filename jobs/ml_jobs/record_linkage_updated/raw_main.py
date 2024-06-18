import multiprocessing as mp
import pandas as pd
import recordlinkage
import networkx as nx
import uuid

FEATURES = {
    "offer_name": {"method": "jarowinkler", "threshold": 0.95},
    "offer_description": {"method": "jarowinkler", "threshold": 0.95},
    "performer": {"method": "jarowinkler", "threshold": 0.95},
}
MATCHES_REQUIRED = 3
CATALOG_PATH = "data_sample_full_nosynchro.parquet"
LINKAGE_PATH = "linkage.parquet"

def clean_catalog(catalog):
    """
    Cleans the catalog dataframe by dropping unnecessary columns and filling null values.
    """
    catalog_clean = catalog.drop(columns=["is_synchronised","name_embedding","description_embedding","booking_confirm_cnt"])
    catalog_clean['performer'] = catalog_clean['performer'].fillna(value='unkn')
    catalog_clean["offer_name"] = catalog_clean["offer_name"].str.lower()
    catalog_clean["offer_description"] = catalog_clean["offer_description"].str.lower()
    return catalog_clean

def load_and_clean_data():
    """
    Loads and cleans the catalog and linkage data.
    """
    catalog = pd.read_parquet(CATALOG_PATH)
    linkage = pd.read_parquet(LINKAGE_PATH)

    catalog_clean = clean_catalog(catalog)

    linkage_clean = linkage[["item_id","link_item_id","batch_id"]]
    table_left = linkage_clean[['item_id',"batch_id"]].drop_duplicates().reset_index(drop=True)
    table_right = linkage_clean[['link_item_id',"batch_id"]].drop_duplicates().reset_index(drop=True)

    return catalog_clean, table_left, table_right

def _setup_matching(c_cl):
    """
    Sets up the matching process by defining the features to be used.
    """
    for feature, feature_dict in FEATURES.items():
        c_cl.string(feature, feature, method=feature_dict["method"], threshold=feature_dict["threshold"], label=feature)
    return c_cl

def get_links(candidate_links,cpr_cl,table_left_enriched,table_right_enriched):
    """
    Computes the links between the left and right tables based on the defined features.
    """
    ftrs = cpr_cl.compute(candidate_links, table_left_enriched, table_right_enriched)
    matches = ftrs[ftrs.sum(axis=1) >= MATCHES_REQUIRED].reset_index()
    matches = matches.rename(columns={"level_0": "index_1", "level_1": "index_2"})
    matches['index_1'] = matches['index_1'].apply(lambda x: f'L-{x}')
    matches['index_2'] = matches['index_2'].apply(lambda x: f'R-{x}')
    return matches  

def chunkify(lst, n):
    """
    Splits a list into n chunks.
    """
    return [lst[i::n] for i in range(n)]

def parallelize_get_links(candidate_links, cpr_cl, table_left_enriched, table_right_enriched):
    """
    Parallelizes the link computation process.
    """
    n_processors = mp.cpu_count()
    chunks = chunkify(candidate_links, n_processors)
    args = [(chunk, cpr_cl, table_left_enriched, table_right_enriched) for chunk in chunks]

    with mp.Pool(processes=n_processors) as pool:
        results = pool.starmap(get_links, args)
    matches = pd.concat(results)
    return matches

def process_linkage(df_matches, table_left_enriched, table_right_enriched):
    """
    Processes the linkage by creating a graph and assigning unique link ids to connected components.
    """
    FG = nx.from_pandas_edgelist(df_matches, source="index_1", target="index_2", edge_attr=True)

    table_left_enriched["link_id"] = ["NC"]*len(table_left_enriched)
    table_right_enriched["link_id"] = ["NC"]*len(table_right_enriched)

    connected_ids = list(nx.connected_components(FG))
    for clusters in range(nx.number_connected_components(FG)):
        link_id = str(uuid.uuid4())[:8]
        for index in connected_ids[clusters]:
            if 'L-' in index:
                index = int(index[2:])
                table_left_enriched.at[index, "link_id"] = str(link_id)
            else:    
                index = int(index[2:])
                table_right_enriched.at[index, "link_id"] = str(link_id)

    table_right_enriched['link_count'] = table_right_enriched.groupby('link_id')['link_id'].transform('count')
    table_right_enriched.to_parquet("linkage_right_final.parquet")
    table_left_enriched.to_parquet("linkage_left_final.parquet")

def main():
    """
    Main function to run the entire process.
    """
    linkage_candidates = pd.read_parquet(LINKAGE_PATH)
    table_left
    catalog_clean, table_left, table_right = load_and_clean_data()

    indexer = recordlinkage.index.Block(on='batch_id')
    candidate_links = indexer.index(table_left, table_right)
    cpr_cl = recordlinkage.Compare()
    cpr_cl = _setup_matching(cpr_cl)

    table_left_enriched = pd.merge(table_left, catalog_clean, on=["item_id"], how="left")
    table_right_enriched = pd.merge(table_right, catalog_clean, left_on=["link_item_id"], right_on=["item_id"], how="left").drop(columns=["link_item_id"])

    matches = parallelize_get_links(candidate_links, cpr_cl, table_left_enriched, table_right_enriched)
    matches.to_parquet("matches_0306.parquet")

    process_linkage(matches, table_left_enriched, table_right_enriched)

if __name__ == "__main__":
    main()