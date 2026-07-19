"""Compute clustering-quality metrics on a linkage run and print a summary.

python evaluation/run_cluster_metrics.py --links ... --similarities ...
"""

import json
from pathlib import Path

import typer
from loguru import logger

from evaluation import data
from evaluation.clustering_metrics import CLUSTER_ID_COL, compute_clustering_metrics

SUMMARY_COLS = [
    "cluster_id",
    "size",
    "density",
    "mean_intra_sim",
    "min_intra_sim",
    "separation",
    "medoid_dispersion",
    "mean_silhouette",
]


DEFAULT_OUT = Path(__file__).resolve().parent / "cluster_metrics.parquet"


def main(
    links: str = typer.Option(data.LINKS_PARQUET, help="Link delta parquet."),
    similarities: str = typer.Option(data.SIMILARITIES_PARQUET, help="Similarities."),
    out: str = typer.Option(str(DEFAULT_OUT), help="Where to write cluster metrics."),
) -> None:
    clusters_df = data.load_links(links).rename(columns={"event_id": CLUSTER_ID_COL})
    similarities_df = data.load_similarities(similarities)

    _, per_cluster, summary = compute_clustering_metrics(clusters_df, similarities_df)
    logger.info("GLOBAL clustering metrics:\n" + json.dumps(summary, indent=2))

    multi = per_cluster[per_cluster["size"] >= 2]
    worst = multi.sort_values("mean_silhouette").head(12)[SUMMARY_COLS]
    logger.info(
        "Worst clusters by silhouette (chaining / mixed):\n"
        + worst.to_string(index=False)
    )
    logger.info(
        f"flags — chained: {int(multi.flag_chained.sum())}, "
        f"weak cohesion: {int(multi.flag_weak_cohesion.sum())}, "
        f"under-merge risk: {int(multi.flag_under_merge.sum())}"
    )

    per_cluster.to_parquet(out, index=False)
    logger.success(f"Wrote per-cluster metrics -> {out}")


if __name__ == "__main__":
    typer.run(main)
