import time
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rapidfuzz
from scipy.cluster.hierarchy import linkage, fcluster
from scipy.spatial.distance import squareform
from scipy.stats import mode
from sklearn.metrics import precision_score, recall_score, f1_score, silhouette_score

# %% Load Data
test_set_df = pd.read_csv("notebooks/author_performer_unicity/test_set.csv").loc[
    lambda df: ~df.true_cluster_name.str.contains("ANOMALY")
]
input_df = test_set_df[["author"]]
# scorer = rapidfuzz.distance.OSA.normalized_distance
scorer = rapidfuzz.distance.Levenshtein.normalized_distance
# scorer = rapidfuzz.distance.LCSseq.normalized_distance
# scorer = rapidfuzz.distance.DamerauLevenshtein.normalized_distance
# scorer = rapidfuzz.distance.Jaro.normalized_distance
# scorer = rapidfuzz.distance.JaroWinkler.normalized_distance
# scorer = rapidfuzz.distance.Indel.normalized_distance


# %% Preprocessing
def preprocessing(string: str) -> str:
    return " ".join(sorted(rapidfuzz.utils.default_process(string).split()))


filtered_df = test_set_df.assign(
    preprocessed_author=lambda df: df.author.map(preprocessing)
)

# %% Compute Distance Matrix
N_WORKERS = -1  # -1 for max number of workers
authors_list = filtered_df.preprocessed_author.dropna().tolist()
t0 = time.time()
distance_matrix = rapidfuzz.process.cdist(
    queries=authors_list,
    choices=authors_list,
    scorer=scorer,
    workers=N_WORKERS,
)
condensed_dist_matrix = squareform(distance_matrix)
Z = linkage(condensed_dist_matrix, "centroid")

# %% Compute Clusters for different distance thresholds
precision_dict = {}
recall_dict = {}
f1_dict = {}
n_clusters_dict = {}
silhouette_dict = {}
f1_silhouette_dict = {}
for distance_threshold in np.arange(0, 1, 0.01):

    clusters = fcluster(Z, distance_threshold, criterion="distance")
    if len(np.unique(clusters)) == 1:
        break

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
    # Calculate Silhouette Score only if there are at least 2 clusters

    silhouette = silhouette_score(distance_matrix, clusters, metric="precomputed")

    precision_dict[distance_threshold] = precision
    recall_dict[distance_threshold] = recall
    f1_dict[distance_threshold] = f1
    n_clusters_dict[distance_threshold] = len(np.unique(clusters))
    silhouette_dict[distance_threshold] = silhouette
    f1_silhouette_dict[distance_threshold] = f1 + silhouette

# %% Display with matplotlib
# Retrieve max f1_silhouette key
argmax_f1_silhouette = max(f1_silhouette_dict, key=f1_silhouette_dict.get)
print("Threshold", argmax_f1_silhouette)
print(f"Best Precision: {precision_dict[argmax_f1_silhouette]:.4f}")
print(f"Best Recall: {recall_dict[argmax_f1_silhouette]:.4f}")
print(f"Best F1 Score: {f1_dict[argmax_f1_silhouette]:.4f}")
print(f"Best Silhouette Score: {silhouette_dict[argmax_f1_silhouette]:.4f}")
print(
    f"Best F1 Silhouette: {f1_dict[argmax_f1_silhouette] + silhouette_dict[argmax_f1_silhouette]:.4f}"
)
print(f"Best Number of clusters: {n_clusters_dict[argmax_f1_silhouette]}")

plt.figure(figsize=(10, 20))
plt.subplot(3, 1, 1)
plt.plot(precision_dict.keys(), precision_dict.values(), label="Precision")
plt.plot(recall_dict.keys(), recall_dict.values(), label="Recall")
plt.plot(f1_dict.keys(), f1_dict.values(), label="F1 Score")
plt.plot(
    f1_silhouette_dict.keys(), f1_silhouette_dict.values(), label="F1 + Silhouette"
)
plt.axvline(
    x=argmax_f1_silhouette, color="r", linestyle="--", label="Max F1 + Silhouette"
)
plt.legend()

plt.subplot(3, 1, 2)
plt.plot(silhouette_dict.keys(), silhouette_dict.values(), label="Silhouette Score")
plt.axvline(
    x=argmax_f1_silhouette, color="r", linestyle="--", label="Max F1 + Silhouette"
)
plt.legend()

plt.subplot(3, 1, 3)
plt.plot(n_clusters_dict.keys(), n_clusters_dict.values(), label="Number of clusters")
plt.axvline(
    x=argmax_f1_silhouette, color="r", linestyle="--", label="Max F1 + Silhouette"
)
plt.legend()
plt.show()


# %%
# Save the results
pd.DataFrame(
    {
        "distance_threshold": list(precision_dict.keys()),
        "precision": list(precision_dict.values()),
        "recall": list(recall_dict.values()),
        "f1": list(f1_dict.values()),
        "silhouette": list(silhouette_dict.values()),
        "f1_silhouette": list(f1_silhouette_dict.values()),
        "n_clusters": list(n_clusters_dict.values()),
    }
).to_csv("notebooks/author_performer_unicity/data/metrics.csv", index=False)
