import time

import pandas as pd
import rapidfuzz
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform

# %% Load Data
author_df = pd.read_csv("notebooks/author_performer_unicity/data/author.csv")
PUNCTUATION = r"!|#|\$|\%|\&|\(|\)|\*|\+|\,|\/|\:|\;|\|\s-|\s-\s|-\s|\|"  # '<=>?@[\\]^_`{|}~|\s–\s'

CATEGORY_ID = "LIVRE"
NAME_FILTER = "oda"


# %% functions
def preprocessing(string: str) -> str:
    return " ".join(sorted(rapidfuzz.utils.default_process(string).split()))


# %%
filtered_df = (
    (
        author_df.dropna()
        .loc[lambda df: df.offer_category_id == CATEGORY_ID]
        .loc[lambda df: ~df.author.str.contains(PUNCTUATION)]
        .loc[lambda df: df.author.str.contains(NAME_FILTER)]
    )
    .assign(preprocessed_author=lambda df: df.author.map(preprocessing))
    .drop(columns=["booking_cnt", "is_synchronised"])
)


N_WORKERS = -1  # -1 for max number of workers
authors_list = filtered_df.preprocessed_author.dropna().tolist()
t0 = time.time()
distance_matrix = rapidfuzz.process.cdist(
    queries=authors_list,
    choices=authors_list,
    scorer=rapidfuzz.distance.OSA.normalized_distance,
    workers=N_WORKERS,
)
condensed_dist_matrix = squareform(distance_matrix)
Z = linkage(condensed_dist_matrix, "centroid")


clusters = fcluster(Z, 0.2, criterion="distance")
clusters_df = (
    pd.DataFrame({"preprocessed_author": authors_list})
    .assign(cluster=clusters)
    .groupby("cluster")
    .agg({"preprocessed_author": set})
    .assign(num_authors_by_cluster=lambda df: df.preprocessed_author.map(len))
    .sort_values("num_authors_by_cluster", ascending=False)
    .assign(cluster_name=lambda df: df.preprocessed_author.map(lambda x: set(x).pop()))
)

# Assign cluster names to clusters
merged_df = pd.merge(
    filtered_df,
    clusters_df.explode("preprocessed_author")[["preprocessed_author", "cluster_name"]],
    on="preprocessed_author",
    how="left",
)

# Save the results
merged_df.drop_duplicates().to_csv(
    "notebooks/author_performer_unicity/data/author_clustered.csv", index=False
)
