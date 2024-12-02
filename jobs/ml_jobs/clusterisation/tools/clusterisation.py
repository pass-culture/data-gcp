import secrets
import typing as t

import numpy as np
import polars as pl
from loguru import logger
from sklearn.cluster import MiniBatchKMeans
from sklearn.metrics import silhouette_samples


def clusterisation_from_prebuild_embedding(
    embedding,
    target_n_clusters: int,
) -> pl.DataFrame:
    """Clusterisation using MBKmeans

    Perform clusterisation using the MBKmeans algorithm on the given embedding.

    Args:
        embedding (array-like): The prebuilt embedding to be clustered.
        target_n_clusters (int): The desired number of clusters.

    Returns:
        polars.DataFrame : A DataFrame containing the items with their assigned clusters and coordinates.
    """
    logger.info("mbkmeans_clusters: clustering...")
    clustering, cluster_labels = mbkmeans_clusters(
        X=embedding,
        k=target_n_clusters,
        mb=10_240,
        print_silhouette_values=False,
    )
    logger.info("mbkmeans_clusters: done...")
    items_with_clusters = pl.DataFrame({"cluster": cluster_labels})
    cluster_center_coordinates = pl.DataFrame(
        {
            "cluster": [ids for ids in range(0, target_n_clusters)],
            "x_cluster": [r[0] for r in list(clustering.cluster_centers_)],
            "y_cluster": [r[1] for r in list(clustering.cluster_centers_)],
        }
    )
    items_with_clusters = items_with_clusters.with_columns(
        cluster=pl.col("cluster").cast(pl.Int32())
    )
    cluster_center_coordinates = cluster_center_coordinates.with_columns(
        cluster=pl.col("cluster").cast(pl.Int32())
    )

    items_with_cluster_and_coordinates = items_with_clusters.join(
        cluster_center_coordinates,
        on="cluster",
        how="inner",
    )
    logger.info("mbkmeans_clusters: exported clusters...")

    return items_with_cluster_and_coordinates


def mbkmeans_clusters(
    X,
    k,
    mb,
    print_silhouette_values=False,
) -> t.Tuple[MiniBatchKMeans, np.ndarray]:
    """Generate clusters and print Silhouette metrics using MBKmeans

    Args:
        X: Matrix of features.
        k: Number of clusters.
        mb: Size of mini-batches.
        print_silhouette_values: Print silhouette values per cluster.

    Returns:
        Trained clustering model and labels based on X.
    """
    seed = secrets.randbelow(1000)
    km = MiniBatchKMeans(n_clusters=k, batch_size=mb, random_state=seed, verbose=1).fit(
        X
    )
    logger.info(f"Random state for MiniBatchKMeans fixed to seed={seed}")
    logger.info(f"For n_clusters = {k}")
    logger.info(f"Inertia:{km.inertia_}")

    if print_silhouette_values:
        sample_silhouette_values = silhouette_samples(X, km.labels_)
        silhouette_values = []
        for i in range(k):
            cluster_silhouette_values = sample_silhouette_values[km.labels_ == i]
            silhouette_values.append(
                (
                    i,
                    cluster_silhouette_values.shape[0],
                    cluster_silhouette_values.mean(),
                    cluster_silhouette_values.min(),
                    cluster_silhouette_values.max(),
                )
            )
        silhouette_values = sorted(
            silhouette_values, key=lambda tup: tup[2], reverse=True
        )
        for s in silhouette_values:
            logger.info(
                f"    Cluster {s[0]}: Size:{s[1]} | Avg:{s[2]:.2f} | Min:{s[3]:.2f} | Max: {s[4]:.2f}"
            )
    return km, km.labels_
