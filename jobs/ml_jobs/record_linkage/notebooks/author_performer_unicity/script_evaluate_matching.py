import time
from collections import defaultdict

import numpy as np
import pandas as pd
import rapidfuzz
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform
from scipy.stats import mode
from sklearn.metrics import precision_score, recall_score, f1_score

# %% Load Data
test_set_df = pd.read_csv("notebooks/author_performer_unicity/test_set.csv").loc[
    lambda df: ~df.true_cluster_name.str.contains("ANOMALY")
]

# %% Preprocess Data
input_df = test_set_df[["author"]]


# %% Preprocessing
def preprocessing(string: str) -> str:
    return " ".join(sorted(rapidfuzz.utils.default_process(string).split()))


filtered_df = test_set_df.assign(
    preprocessed_author=lambda df: df.author.map(preprocessing)
)


# %% Clustering
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

# %% Metrics
ground_truth_labels = filtered_df.true_cluster_name.tolist()
cluster_to_gt = defaultdict(list)
for label, gt in zip(clusters, ground_truth_labels):
    cluster_to_gt[label].append(gt)


# Assign the most common ground truth label in each cluster to all members of that cluster
predicted_labels = np.empty_like(clusters, dtype=object)
for cluster, gt_labels in cluster_to_gt.items():
    common_gt = mode(gt_labels).mode[0]
    predicted_labels[clusters == cluster] = common_gt

# Calculate Precision, Recall, and F1 score
precision = precision_score(
    ground_truth_labels,
    predicted_labels,
    average="macro",
    labels=np.unique(ground_truth_labels),
)
recall = recall_score(
    ground_truth_labels,
    predicted_labels,
    average="macro",
    labels=np.unique(ground_truth_labels),
)
f1 = f1_score(
    ground_truth_labels,
    predicted_labels,
    average="macro",
    labels=np.unique(ground_truth_labels),
)

print(f"Precision: {precision:.4f}")
print(f"Recall: {recall:.4f}")
print(f"F1 Score: {f1:.4f}")
