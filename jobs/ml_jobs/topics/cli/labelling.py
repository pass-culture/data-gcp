"""c-TF-IDF labelling, optional outlier reassignment, and BigQuery export
(spec section 7.4).

CLI commands stay file-based (parquet in/out); only ``export`` touches
BigQuery. Run from the job root:

    uv run python main.py labelling ctfidf \\
        --documents-file-path documents.parquet \\
        --clusters-file-path clusters.parquet \\
        --topics-output-file-path topics.parquet \\
        --item-topic-output-file-path item_topic.parquet \\
        --split-level 1 --corpus-id 01000000 --corpus-label Littérature
"""

from datetime import UTC, datetime

import polars as pl
import typer
from google.cloud import bigquery
from loguru import logger

from src.common.bigquery import merge_upsert, read_dataframe, write_dataframe
from src.common.config import CONFIGS_PATH, load_config
from src.common.constants import (
    BIGQUERY_ML_FEATURES_DATASET,
    GCP_PROJECT_ID,
    MLFLOW_RUN_ID_COLUMN,
)
from src.common.corpora import corpus_depth
from src.common.mlflow import log_config_to_run
from src.common.run import read_run_id
from src.common.schemas import ITEM_TOPIC_SCHEMA, TOPIC_SCHEMA
from src.labelling.ctfidf import (
    build_count_matrix,
    compute_ctfidf,
    compute_idf,
    select_corpus_vocabulary,
    summarize_topics,
)
from src.labelling.reassignment import find_reassignment, transform_outlier_vector

app = typer.Typer(help="c-TF-IDF labelling, reassignment, and BigQuery export.")


def _read_vocabulary(config_min_df: int, config_max_features: int) -> list[str]:
    vocabulary_table = read_dataframe(
        "select term, df_total, is_stop_word from "
        f"`{GCP_PROJECT_ID}.{BIGQUERY_ML_FEATURES_DATASET}.topic_vocabulary`"
    )
    return select_corpus_vocabulary(
        vocabulary_table, config_min_df, config_max_features
    )


def _joined_documents(
    documents_file_path: str, clusters_file_path: str
) -> pl.DataFrame:
    documents = pl.read_parquet(documents_file_path)
    clusters = pl.read_parquet(clusters_file_path)
    return documents.join(
        clusters.select("item_id", "topic_uuid", "is_outlier"),
        on="item_id",
        how="inner",
    )


@app.command()
def ctfidf(
    documents_file_path: str = typer.Option(
        ..., help="This corpus's topic_item_document rows (item_id, gtl_id, document)."
    ),
    clusters_file_path: str = typer.Option(
        ..., help="This corpus's topic_item_cluster rows from `clustering fit`."
    ),
    topics_output_file_path: str = typer.Option(
        ..., help="Where to write the topic rows (pre-export)."
    ),
    item_topic_output_file_path: str = typer.Option(
        ..., help="Where to write the item_topic rows (pre-export)."
    ),
    split_level: int = typer.Option(..., help="This corpus's split level."),
    corpus_id: str = typer.Option(..., help="This corpus's id."),
    corpus_label: str = typer.Option(
        ..., help="The GTL label path, e.g. 'Littérature'."
    ),
    config_file_name: str = typer.Option("default"),
) -> None:
    config = load_config(config_file_name)
    vocabulary = _read_vocabulary(
        config.labelling.min_df, config.labelling.max_features
    )
    joined = _joined_documents(documents_file_path, clusters_file_path)

    topics = summarize_topics(
        joined,
        vocabulary,
        split_level=split_level,
        corpus_id=corpus_id,
        corpus_label=corpus_label,
        corpus_depth=corpus_depth(corpus_id),
        top_n_words=config.labelling.top_n_words,
        label_words=config.labelling.label_words,
        reduce_frequent_words=config.labelling.reduce_frequent_words,
    )
    topics.write_parquet(topics_output_file_path)

    item_topic = (
        pl.read_parquet(clusters_file_path)
        .select("item_id", "topic_uuid", "is_outlier", "assignment")
        .with_columns(
            pl.lit(split_level).alias("split_level"),
            pl.lit(corpus_id).alias("corpus_id"),
            pl.col("is_outlier").alias("original_is_outlier"),
        )
    )
    item_topic.write_parquet(item_topic_output_file_path)

    logger.info(
        f"Labelled {topics.height} topics from {joined.height} items "
        f"-> {topics_output_file_path}"
    )


@app.command()
def reassign(
    documents_file_path: str = typer.Option(..., help="Same input as `ctfidf`."),
    clusters_file_path: str = typer.Option(..., help="Same input as `ctfidf`."),
    item_topic_file_path: str = typer.Option(
        ..., help="item_topic rows from `ctfidf`, to update."
    ),
    output_file_path: str = typer.Option(..., help="Where to write the updated rows."),
    config_file_name: str = typer.Option("default"),
) -> None:
    config = load_config(config_file_name)
    item_topic = pl.read_parquet(item_topic_file_path)

    if not config.labelling.reassign_outliers:
        logger.info("reassign_outliers is off; item_topic passed through unchanged.")
        item_topic.write_parquet(output_file_path)
        return

    vocabulary = _read_vocabulary(
        config.labelling.min_df, config.labelling.max_features
    )
    joined = _joined_documents(documents_file_path, clusters_file_path)

    # The outlier group is included as one row for idf/A purposes (spec
    # step 3), but it is never itself a valid reassignment target -- an
    # outlier's own vector is trivially similar to its current group.
    topic_ids = joined["topic_uuid"].unique().to_list()
    count_matrix = build_count_matrix(joined, topic_ids, vocabulary)
    idf = compute_idf(count_matrix)
    scores = compute_ctfidf(
        count_matrix, reduce_frequent_words=config.labelling.reduce_frequent_words
    )

    outlier_topic_uuids = set(
        joined.filter(pl.col("is_outlier"))["topic_uuid"].unique().to_list()
    )
    candidate_mask = [topic_id not in outlier_topic_uuids for topic_id in topic_ids]
    candidate_topic_ids = [
        topic_id
        for topic_id, keep in zip(topic_ids, candidate_mask, strict=True)
        if keep
    ]
    candidate_scores = scores[candidate_mask]

    outliers = joined.filter(pl.col("is_outlier"))
    reassigned_to: dict[str, str] = {}
    for item_id, document in zip(
        outliers["item_id"].to_list(), outliers["document"].to_list(), strict=True
    ):
        outlier_vector = transform_outlier_vector(document, vocabulary, idf)
        new_topic = find_reassignment(
            outlier_vector,
            candidate_scores,
            candidate_topic_ids,
            min_shared_words=config.labelling.min_shared_words,
        )
        if new_topic is not None:
            reassigned_to[item_id] = new_topic

    reassignment_df = (
        pl.DataFrame(
            {
                "item_id": list(reassigned_to.keys()),
                "new_topic_uuid": list(reassigned_to.values()),
            }
        )
        if reassigned_to
        else pl.DataFrame(schema={"item_id": pl.String, "new_topic_uuid": pl.String})
    )

    result = (
        item_topic.join(reassignment_df, on="item_id", how="left")
        .with_columns(
            pl.coalesce(["new_topic_uuid", "topic_uuid"]).alias("topic_uuid"),
            pl.when(pl.col("new_topic_uuid").is_not_null())
            .then(pl.lit("reassigned"))
            .otherwise(pl.col("assignment"))
            .alias("assignment"),
            pl.when(pl.col("new_topic_uuid").is_not_null())
            .then(pl.lit(False))
            .otherwise(pl.col("is_outlier"))
            .alias("is_outlier"),
        )
        .drop("new_topic_uuid")
    )
    result.write_parquet(output_file_path)
    logger.info(f"Reassigned {len(reassigned_to)} outlier(s) -> {output_file_path}")


@app.command()
def export(
    topics_file_path: str = typer.Option(..., help="topic rows from `ctfidf`."),
    item_topic_file_path: str = typer.Option(
        ..., help="item_topic rows from `ctfidf`/`reassign`."
    ),
    full_refit: bool = typer.Option(  # noqa: FBT001
        False, help="Truncate and reload instead of merging."
    ),
    run_id: str | None = typer.Option(None, help="Defaults to the shared run file."),
    config_file_name: str = typer.Option("default"),
) -> None:
    config = load_config(config_file_name)
    resolved_run_id = run_id or read_run_id()
    now = datetime.now(UTC)

    topics = pl.read_parquet(topics_file_path).with_columns(
        pl.lit(resolved_run_id).alias("run_id"),
        pl.lit(now).alias("created_at"),
        pl.lit(resolved_run_id).alias(MLFLOW_RUN_ID_COLUMN),
    )
    item_topic = pl.read_parquet(item_topic_file_path).with_columns(
        pl.lit(resolved_run_id).alias("run_id"),
        pl.lit(resolved_run_id).alias(MLFLOW_RUN_ID_COLUMN),
    )

    if full_refit:
        write_dataframe(
            topics,
            BIGQUERY_ML_FEATURES_DATASET,
            "topic",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=TOPIC_SCHEMA,
        )
        write_dataframe(
            item_topic,
            BIGQUERY_ML_FEATURES_DATASET,
            "item_topic",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=ITEM_TOPIC_SCHEMA,
        )
    else:
        merge_upsert(
            topics,
            BIGQUERY_ML_FEATURES_DATASET,
            "topic",
            key_columns=["topic_uuid"],
            schema=TOPIC_SCHEMA,
        )
        merge_upsert(
            item_topic,
            BIGQUERY_ML_FEATURES_DATASET,
            "item_topic",
            key_columns=["item_id"],
            schema=ITEM_TOPIC_SCHEMA,
        )

    log_config_to_run(
        resolved_run_id,
        "labelling",
        config.labelling,
        str(CONFIGS_PATH / f"{config_file_name}.yaml"),
        metrics={"n_topics": topics.height, "n_items": item_topic.height},
    )
    logger.info(
        f"Exported {topics.height} topics and {item_topic.height} item "
        f"assignments (full_refit={full_refit})."
    )


if __name__ == "__main__":
    app()
