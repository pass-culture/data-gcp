"""Unsupervised cluster-quality metrics for event linkage.

Clusters are connected components of a similarity graph, so offers have no coordinate
representation and the usual centroid-based metrics do not apply. This module computes
their similarity-native analogs (silhouette, medoid dispersion, cohesion, density,
separation) from a precomputed distance = 1 - sim.

See the README in this directory for what each metric means and how to read it.
Entry point: `compute_clustering_metrics(clusters_df, similarities_df)`.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants import (
    DESCRIPTION_SIMILARITY_COL,
    IMAGE_SIMILARITY_COL,
    NAME_SIMILARITY_COL,
    OFFER_ID_COL,
)

CLUSTER_ID_COL = "cluster_id"

# Tunable: these drive the summary flags only, never the raw metrics.
DENSITY_CHAINED_THRESHOLD = 0.5
COHESION_WEAK_THRESHOLD = 0.7
SEPARATION_MERGE_THRESHOLD = 0.8


def build_combined_similarity(similarities_df: pd.DataFrame) -> pd.DataFrame:
    """Collapse the similarity signals into one score per pair, both directions.

    Args:
        similarities_df: pairwise table with offer_id_1 / offer_id_2 and the
            name / description / image similarity columns.

    Returns:
        Long dataframe with columns [i, j, sim] where sim in [0, 1] is
        max(name/100, description/100, image). Each unordered pair appears twice
        (i, j) and (j, i) so per-offer aggregation is a simple groupby.
    """
    name = similarities_df[NAME_SIMILARITY_COL].fillna(0).to_numpy() / 100.0
    desc = similarities_df[DESCRIPTION_SIMILARITY_COL].fillna(0).to_numpy() / 100.0
    img = similarities_df[IMAGE_SIMILARITY_COL].fillna(0).to_numpy()
    sim = np.maximum.reduce([name, desc, img])

    base = pd.DataFrame(
        {
            "i": similarities_df[f"{OFFER_ID_COL}_1"].to_numpy(),
            "j": similarities_df[f"{OFFER_ID_COL}_2"].to_numpy(),
            "sim": sim,
        }
    )
    flipped = base.rename(columns={"i": "j", "j": "i"})
    return pd.concat([base, flipped], ignore_index=True)


def compute_offer_silhouette(
    cluster_of: pd.Series, sim_long: pd.DataFrame, sizes: pd.Series
) -> pd.DataFrame:
    """Silhouette per offer using precomputed distances, exploiting sparsity.

    A missing pair means similarity 0 (distance 1), so the mean distance from offer i
    to a cluster C is 1 - (sum of observed sim to C) / |C|. This lets us compute a(i)
    and b(i) without materialising a dense N x N distance matrix.

    Returns a dataframe indexed by offer_id with columns a, b, silhouette
    (singletons get silhouette 0, matching sklearn's convention).
    """
    df = sim_long.copy()
    df["ci"] = df["i"].map(cluster_of)
    df["cj"] = df["j"].map(cluster_of)
    df = df.dropna(subset=["ci", "cj"])

    intra = df[df.ci == df.cj]
    inter = df[df.ci != df.cj]

    sum_intra = intra.groupby("i")["sim"].sum()
    size_ci = cluster_of.map(sizes)
    a = 1.0 - sum_intra.div(size_ci.loc[sum_intra.index].to_numpy() - 1)

    inter_cluster = inter.groupby(["i", "cj"])["sim"].sum().reset_index()
    other_sizes = inter_cluster["cj"].map(sizes).to_numpy()
    inter_cluster["mean_sim"] = inter_cluster["sim"] / other_sizes
    best_ext = inter_cluster.groupby("i")["mean_sim"].max()
    b = 1.0 - best_ext

    offers = pd.Index(cluster_of.index, name=OFFER_ID_COL)
    out = pd.DataFrame(index=offers)
    out["a"] = a.reindex(offers)
    # An offer with no external edge is perfectly separated.
    out["b"] = b.reindex(offers).fillna(1.0)
    is_multi = cluster_of.map(sizes).reindex(offers) > 1
    out.loc[~is_multi, ["a", "b"]] = np.nan

    denom = out[["a", "b"]].max(axis=1)
    out["silhouette"] = ((out["b"] - out["a"]) / denom).where(denom > 0, 0.0)
    out.loc[~is_multi, "silhouette"] = 0.0  # sklearn convention for singletons
    return out


def compute_cluster_metrics(
    clusters_df: pd.DataFrame, similarities_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute per-offer and per-cluster quality metrics.

    Args:
        clusters_df: rows of (offer_id, cluster_id) — the current linkage assignment.
        similarities_df: pairwise similarity table (raw similarities parquet schema).

    Returns:
        (per_offer_df, per_cluster_df).
    """
    cluster_of = clusters_df.set_index(OFFER_ID_COL)[CLUSTER_ID_COL]
    sizes = clusters_df.groupby(CLUSTER_ID_COL)[OFFER_ID_COL].size()

    sim_long = build_combined_similarity(similarities_df)
    tracked = cluster_of.index
    sim_long = sim_long[sim_long.i.isin(tracked) & sim_long.j.isin(tracked)]
    sim_long["ci"] = sim_long["i"].map(cluster_of)
    sim_long["cj"] = sim_long["j"].map(cluster_of)

    per_offer = compute_offer_silhouette(cluster_of, sim_long, sizes)
    per_offer[CLUSTER_ID_COL] = cluster_of.reindex(per_offer.index).to_numpy()

    intra = sim_long[(sim_long.ci == sim_long.cj) & (sim_long.i < sim_long.j)]
    grp = intra.groupby("ci")
    n_present = grp.size()
    mean_sim = grp["sim"].mean()
    min_sim = grp["sim"].min()

    inter = sim_long[sim_long.ci != sim_long.cj]
    separation = inter.groupby("ci")["sim"].max()

    medoid_disp = _medoid_dispersion(intra, sizes)

    n_possible = sizes * (sizes - 1) / 2
    per_cluster = pd.DataFrame({"size": sizes})
    density = n_present.reindex(sizes.index).fillna(0) / n_possible
    per_cluster["density"] = density.clip(upper=1.0)
    per_cluster["mean_intra_sim"] = mean_sim.reindex(sizes.index)
    per_cluster["min_intra_sim"] = min_sim.reindex(sizes.index)
    per_cluster["medoid_dispersion"] = medoid_disp.reindex(sizes.index)
    per_cluster["separation"] = separation.reindex(sizes.index).fillna(0.0)
    silhouette = per_offer.groupby(CLUSTER_ID_COL)["silhouette"].mean()
    per_cluster["mean_silhouette"] = silhouette

    # By convention a singleton is perfectly cohesive: it has no intra pair to score.
    singleton = per_cluster["size"] == 1
    per_cluster.loc[singleton, ["density", "mean_intra_sim", "min_intra_sim"]] = 1.0
    per_cluster.loc[singleton, ["medoid_dispersion"]] = 0.0

    per_cluster["flag_chained"] = (per_cluster["size"] >= 3) & (
        per_cluster["density"] < DENSITY_CHAINED_THRESHOLD
    )
    per_cluster["flag_weak_cohesion"] = (
        per_cluster["mean_intra_sim"] < COHESION_WEAK_THRESHOLD
    )
    per_cluster["flag_under_merge"] = (
        per_cluster["separation"] >= SEPARATION_MERGE_THRESHOLD
    )
    per_cluster = per_cluster.reset_index().rename(columns={"index": CLUSTER_ID_COL})
    return per_offer, per_cluster


def _medoid_dispersion(intra: pd.DataFrame, sizes: pd.Series) -> pd.Series:
    """Mean squared distance (1 - sim)^2 of cluster members to the cluster medoid.

    The medoid is the offer with the greatest total intra-cluster similarity. Missing
    pairs contribute distance 1. This is the similarity-space analog of k-means inertia.
    """
    if intra.empty:
        return pd.Series(dtype=float)
    both = pd.concat(
        [
            intra[["ci", "i", "j", "sim"]],
            intra.rename(columns={"i": "j", "j": "i"})[["ci", "i", "j", "sim"]],
        ]
    )
    centrality = both.groupby(["ci", "i"])["sim"].sum()
    medoid = centrality.groupby("ci").idxmax().map(lambda t: t[1])

    disp = {}
    for cid, med in medoid.items():
        n = int(sizes[cid])
        sims_to_med = both[(both.ci == cid) & (both.i == med)].set_index("j")["sim"]
        members = pd.Index([o for o in _cluster_members(both, cid) if o != med])
        d2 = (1.0 - sims_to_med.reindex(members).fillna(0.0)) ** 2
        disp[cid] = float(d2.sum() / max(n - 1, 1))
    return pd.Series(disp)


def _cluster_members(both: pd.DataFrame, cid) -> set:
    sub = both[both.ci == cid]
    return set(sub["i"]).union(sub["j"])


def compute_global_metrics(per_offer: pd.DataFrame, per_cluster: pd.DataFrame) -> dict:
    """Roll the per-cluster metrics into a single monitorable summary dict."""
    multi = per_cluster[per_cluster["size"] >= 2]
    offers_multi = per_offer[
        per_offer[CLUSTER_ID_COL].map(per_cluster.set_index(CLUSTER_ID_COL)["size"])
        >= 2
    ]
    return {
        "n_clusters": int((per_cluster["size"] >= 2).sum()),
        "n_singletons": int((per_cluster["size"] == 1).sum()),
        "n_offers_clustered": int(multi["size"].sum()),
        "mean_silhouette": _r(offers_multi["silhouette"].mean()),
        "median_silhouette": _r(offers_multi["silhouette"].median()),
        "mean_cohesion": _r(multi["mean_intra_sim"].mean()),
        "mean_density": _r(multi["density"].mean()),
        "mean_medoid_dispersion": _r(multi["medoid_dispersion"].mean()),
        "pct_chained_clusters": _r(multi["flag_chained"].mean()),
        "pct_weak_cohesion_clusters": _r(multi["flag_weak_cohesion"].mean()),
        "pct_under_merge_clusters": _r(multi["flag_under_merge"].mean()),
    }


def _r(x) -> float | None:
    return None if pd.isna(x) else round(float(x), 4)


def compute_clustering_metrics(
    clusters_df: pd.DataFrame, similarities_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """Entry point: per-offer metrics, per-cluster metrics, and the global summary."""
    per_offer, per_cluster = compute_cluster_metrics(clusters_df, similarities_df)
    return per_offer, per_cluster, compute_global_metrics(per_offer, per_cluster)
