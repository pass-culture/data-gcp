import recordlinkage
import multiprocessing as mp
import pandas as pd
import uuid
import networkx as nx
FEATURES = {
    "offer_name": {"method": "jarowinkler", "threshold": 0.95},
    "offer_description": {"method": "jarowinkler", "threshold": 0.95},
    "performer": {"method": "jarowinkler", "threshold": 0.95},
}
MATCHES_REQUIRED = 3

def setup_matching():
    """
    Sets up the matching process by defining the features to be used.
    """
    cpr_cl = recordlinkage.Compare()
    for feature, feature_dict in FEATURES.items():
        cpr_cl.string(feature, feature, method=feature_dict["method"], threshold=feature_dict["threshold"], label=feature)
    return cpr_cl

def get_links(candidate_links,cpr_cl,table_left,table_right):
    """
    Computes the links between the left and right tables based on the defined features.
    """
    ftrs = cpr_cl.compute(candidate_links, table_left, table_right)
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

def multiprocess_matching(candidate_links, cpr_cl, table_left, table_right):
    """
    Parallelizes the link computation process.
    """
    n_processors = mp.cpu_count()
    chunks = chunkify(candidate_links, n_processors)
    args = [(chunk, cpr_cl, table_left, table_right) for chunk in chunks]

    with mp.Pool(processes=n_processors) as pool:
        results = pool.starmap(get_links, args)
    matches = pd.concat(results)
    return matches

def link_matches_graph(df_matches, table_left, table_right):
    """
    Processes the linkage by creating a graph and assigning unique link ids to connected components.
    """
    FG = nx.from_pandas_edgelist(df_matches, source="index_1", target="index_2", edge_attr=True)

    table_left["link_id"] = ["NC"]*len(table_left)
    table_right["link_id"] = ["NC"]*len(table_right)

    connected_ids = list(nx.connected_components(FG))
    for clusters in range(nx.number_connected_components(FG)):
        link_id = str(uuid.uuid4())[:8]
        for index in connected_ids[clusters]:
            if 'L-' in index:
                index = int(index[2:])
                table_left.at[index, "link_id"] = str(link_id)
            else:    
                index = int(index[2:])
                table_right.at[index, "link_id"] = str(link_id)

    table_right['link_count'] = table_right.groupby('link_id')['link_id'].transform('count')
    return table_right, table_left
