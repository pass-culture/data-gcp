"""HDBSCAN clustering, topic UUID carryover, and incremental prediction
(spec section 7.3).

CLI commands stay file-based (parquet in/out); the fitted model goes to GCS.
Writes ``topic_item_cluster`` as a parquet file -- it's an intermediate
artifact consumed only by ``labelling``, not one of the spec's three
published BigQuery tables. Run from the job root:

    uv run python main.py clustering fit \\
        --input-file-path vectors.parquet --output-file-path clusters.parquet \\
        --split-level 1 --corpus-id 01000000 --run-id abc123
"""

import numpy as np
import polars as pl
import typer
from loguru import logger

from src.clustering.hdbscan_models import fit_hdbscan, predict_clusters
from src.clustering.uuid_matching import (
    OUTLIER_LABEL,
    cluster_item_sets,
    match_uuids,
    outlier_uuid,
)
from src.common import gcs
from src.common.config import load_config

app = typer.Typer(help="HDBSCAN clustering on reduced corpus vectors.")

MODEL_BLOB_NAME = "hdbscan_model.pkl"


def _load_old_clusters(previous_clusters_file_path: str | None) -> dict[str, set[str]]:
    if not previous_clusters_file_path:
        return {}
    previous = pl.read_parquet(previous_clusters_file_path).filter(
        ~pl.col("is_outlier")
    )
    old_clusters: dict[str, set[str]] = {}
    for (topic_uuid,), group in previous.group_by("topic_uuid"):
        old_clusters[topic_uuid] = set(group["item_id"].to_list())
    return old_clusters


@app.command()
def fit(
    input_file_path: str = typer.Option(
        ..., help="5-d vectors parquet from `reduction fit`."
    ),
    output_file_path: str = typer.Option(
        ..., help="Where to write the topic_item_cluster rows."
    ),
    split_level: int = typer.Option(..., help="This corpus's split level."),
    corpus_id: str = typer.Option(..., help="This corpus's id."),
    run_id: str = typer.Option(
        ..., help="This run's id (used in the model's GCS path)."
    ),
    previous_clusters_file_path: str | None = typer.Option(
        None,
        help="The previous refit's topic_item_cluster output, for UUID "
        "carryover. Omit on a first run.",
    ),
    monitoring_file_path: str | None = typer.Option(
        None, help="Where to write the old/new UUID Jaccard mapping."
    ),
    config_file_name: str = typer.Option("default"),
    device: str = typer.Option(
        "gpu", help="'gpu' (cuML, with CPU fallback on OOM) or 'cpu'."
    ),
) -> None:
    config = load_config(config_file_name)
    data = pl.read_parquet(input_file_path)
    vectors = np.asarray(data["vector"].to_list(), dtype=np.float32)
    item_ids = data["item_id"].to_list()

    model = fit_hdbscan(vectors, config.clustering, device=device)
    labels: list[int] = model.labels_.tolist()
    probabilities: list[float] = model.probabilities_.tolist()

    new_clusters = cluster_item_sets(item_ids, labels)
    old_clusters = _load_old_clusters(previous_clusters_file_path)
    assignments, monitoring = match_uuids(
        old_clusters, new_clusters, threshold=config.clustering.uuid_match_jaccard
    )
    this_outlier_uuid = outlier_uuid(corpus_id)
    topic_uuids = [
        this_outlier_uuid if label == OUTLIER_LABEL else assignments[label]
        for label in labels
    ]

    result = pl.DataFrame(
        {
            "item_id": item_ids,
            "split_level": split_level,
            "corpus_id": corpus_id,
            "cluster_id": labels,
            "topic_uuid": topic_uuids,
            "is_outlier": [label == OUTLIER_LABEL for label in labels],
            "membership_probability": probabilities,
            "assignment": "fit",
            "run_id": run_id,
        }
    )

    model_path = gcs.model_path(split_level, corpus_id, run_id) + MODEL_BLOB_NAME
    gcs.save_model(model, model_path)
    gcs.write_latest_pointer(split_level, corpus_id, run_id)

    result.write_parquet(output_file_path)
    if monitoring_file_path:
        pl.DataFrame(monitoring).write_parquet(monitoring_file_path)

    n_outliers = labels.count(OUTLIER_LABEL)
    logger.info(
        f"Fit HDBSCAN on {len(item_ids)} items: {len(new_clusters)} clusters, "
        f"{n_outliers} outliers -> {output_file_path}"
    )


@app.command()
def predict(
    input_file_path: str = typer.Option(
        ..., help="New items' vectors parquet to assign."
    ),
    output_file_path: str = typer.Option(
        ..., help="Where to write their topic_item_cluster rows."
    ),
    cluster_mapping_file_path: str = typer.Option(
        ...,
        help="The fit run's topic_item_cluster output (for cluster_id -> topic_uuid).",
    ),
    split_level: int = typer.Option(..., help="This corpus's split level."),
    corpus_id: str = typer.Option(..., help="This corpus's id."),
    run_id: str = typer.Option(..., help="Which saved model's run to use."),
    device: str = typer.Option("gpu", help="Must match the model's own fit device."),
) -> None:
    model_path = gcs.model_path(split_level, corpus_id, run_id) + MODEL_BLOB_NAME
    model = gcs.load_model(model_path)

    data = pl.read_parquet(input_file_path)
    vectors = np.asarray(data["vector"].to_list(), dtype=np.float32)
    labels, strengths = predict_clusters(model, vectors, device=device)

    cluster_mapping = pl.read_parquet(cluster_mapping_file_path).select(
        "cluster_id", "topic_uuid"
    )
    label_to_uuid = dict(
        zip(
            cluster_mapping["cluster_id"].to_list(),
            cluster_mapping["topic_uuid"].to_list(),
            strict=False,
        )
    )
    this_outlier_uuid = outlier_uuid(corpus_id)
    topic_uuids = [
        this_outlier_uuid
        if label == OUTLIER_LABEL
        else label_to_uuid.get(label, this_outlier_uuid)
        for label in labels
    ]

    result = pl.DataFrame(
        {
            "item_id": data["item_id"],
            "split_level": split_level,
            "corpus_id": corpus_id,
            "cluster_id": labels,
            "topic_uuid": topic_uuids,
            "is_outlier": [label == OUTLIER_LABEL for label in labels],
            "membership_probability": strengths,
            "assignment": "predict",
            "run_id": run_id,
        }
    )
    result.write_parquet(output_file_path)
    logger.info(f"Predicted clusters for {len(data)} items -> {output_file_path}")


if __name__ == "__main__":
    app()
