"""Vocabulary modelling: turn each description into a clean document and keep
a shared word registry (spec section 7.1).

CLI commands stay thin (parquet in/out); only ``update-registry`` touches
BigQuery. Run from the job root, e.g.:

    uv run python main.py vocabulary find-boilerplate \\
        --input-file-path items.parquet --output-file-path boilerplate.json

    uv run python main.py vocabulary select-delta \\
        --input-file-path items.parquet --boilerplate-file-path boilerplate.json \\
        --output-file-path delta.parquet --processing-version v1
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import typer
from loguru import logger

from src.common.config import CONFIGS_PATH, apply_overrides, load_config
from src.common.constants import MLFLOW_RUN_ID_COLUMN
from src.common.mlflow import log_config_to_run
from src.common.run import read_run_id
from src.vocabulary.cleaning import build_boilerplate_pattern, clean_descriptions
from src.vocabulary.cleaning import (
    find_boilerplate_segments as _find_boilerplate_segments,
)
from src.vocabulary.delta import (
    DESCRIPTION_HASH_COLUMN,
    hash_descriptions,
    select_delta,
)
from src.vocabulary.filtering import filter_documents
from src.vocabulary.lemmatization import lemmatize_documents
from src.vocabulary.pruning import build_document, build_stop_words, prune_lemmas

app = typer.Typer(help="Vocabulary modelling: cleaning, lemmatization, registry.")


@app.command("find-boilerplate")
def find_boilerplate(
    input_file_path: str = typer.Option(
        ..., help="Parquet with an offer_description column (full corpus)."
    ),
    output_file_path: str = typer.Option(
        ..., help="Where to write the boilerplate segments JSON."
    ),
    config_file_name: str = typer.Option("default"),
    min_segment_chars: int | None = typer.Option(None),
    min_share: float | None = typer.Option(None),
    max_texts: int | None = typer.Option(None),
    seed: int | None = typer.Option(None),
) -> None:
    config = load_config(config_file_name)
    boilerplate_config = apply_overrides(
        config.vocabulary.boilerplate,
        {
            "min_segment_chars": min_segment_chars,
            "min_share": min_share,
            "max_texts": max_texts,
            "seed": seed,
        },
    )

    items = pl.read_parquet(input_file_path)
    descriptions = items["offer_description"].drop_nulls().to_list()
    segments = _find_boilerplate_segments(
        descriptions,
        min_segment_chars=boilerplate_config.min_segment_chars,
        min_share=boilerplate_config.min_share,
        max_texts=boilerplate_config.max_texts,
        seed=boilerplate_config.seed,
    )

    Path(output_file_path).write_text(
        json.dumps(segments, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"Found {len(segments)} boilerplate segments -> {output_file_path}")


@app.command("select-delta")
def select_delta_command(
    input_file_path: str = typer.Option(
        ...,
        help=(
            "Parquet with the current raw items (item_id, gtl_type, gtl_id, "
            "gtl_label_level_1, offer_description)."
        ),
    ),
    boilerplate_file_path: str = typer.Option(
        ..., help="boilerplate_segments.json from the last find-boilerplate run."
    ),
    output_file_path: str = typer.Option(
        ...,
        help="Delta to reprocess: item_id, cleaned_description, "
        "description_hash, is_deleted.",
    ),
    processing_version: str = typer.Option(..., help="This run's processing version."),
    previous_documents_file_path: str | None = typer.Option(
        None,
        help="Parquet export of topic_item_document (item_id, description_hash). "
        "Omit on a first run.",
    ),
    previous_processing_version: str | None = typer.Option(
        None, help="processing_version stored in topic_item_document."
    ),
    config_file_name: str = typer.Option("default"),
) -> None:
    config = load_config(config_file_name)

    items = filter_documents(pl.read_parquet(input_file_path))
    boilerplate_segments = json.loads(
        Path(boilerplate_file_path).read_text(encoding="utf-8")
    )
    pattern = build_boilerplate_pattern(boilerplate_segments)

    cleaned = clean_descriptions(items["offer_description"], pattern)
    current_documents = (
        items.with_columns(cleaned_description=cleaned)
        .filter(
            pl.col("cleaned_description").str.len_chars()
            >= config.vocabulary.min_description_chars
        )
        .with_columns(
            hash_descriptions(pl.col("cleaned_description")).alias(
                DESCRIPTION_HASH_COLUMN
            )
        )
    )

    previous_documents = (
        pl.read_parquet(previous_documents_file_path)
        if previous_documents_file_path
        else None
    )

    delta_ids = select_delta(
        current_documents.select("item_id", DESCRIPTION_HASH_COLUMN),
        previous_documents,
        processing_version=processing_version,
        previous_processing_version=previous_processing_version,
    )

    # Left join from delta_ids (not an inner join on current_documents): a
    # deleted item has no row in current_documents at all, and must still
    # make it into the output so downstream steps can flag it is_deleted.
    delta = delta_ids.join(current_documents, on="item_id", how="left")
    delta.write_parquet(output_file_path)

    n_deleted = delta_ids.filter(pl.col("is_deleted")).height
    logger.info(
        f"Selected {delta.height} items to (re)process "
        f"({n_deleted} deleted) -> {output_file_path}"
    )


@app.command()
def process(
    input_file_path: str = typer.Option(..., help="Delta parquet from select-delta."),
    output_file_path: str = typer.Option(
        ..., help="Processed documents, ready for update-registry."
    ),
    processing_version: str = typer.Option(..., help="This run's processing version."),
    config_file_name: str = typer.Option("default"),
    run_id: str | None = typer.Option(
        None,
        help="Pipeline run id for mlflow_run_id provenance. Defaults to the "
        "shared run file written by `common start`.",
    ),
) -> None:
    config = load_config(config_file_name)
    resolved_run_id = run_id or read_run_id()

    delta = pl.read_parquet(input_file_path).with_columns(
        pl.lit(processing_version).alias("processing_version"),
        pl.lit(datetime.now(UTC)).alias("processed_at"),
        pl.lit(resolved_run_id).alias(MLFLOW_RUN_ID_COLUMN),
    )
    to_process = delta.filter(~pl.col("is_deleted"))
    deleted = delta.filter(pl.col("is_deleted"))

    stop_words = build_stop_words(
        config.vocabulary.generic_stop_words, config.vocabulary.forbidden_words
    )
    lemma_pairs = lemmatize_documents(
        to_process["cleaned_description"].to_list(),
        spacy_model=config.vocabulary.spacy_model,
        pos=set(config.vocabulary.pos),
        enable_ner=config.vocabulary.ner,
        chunk_texts=config.vocabulary.chunk_texts,
    )
    noun_lemmas = [prune_lemmas(nouns, stop_words) for nouns, _ in lemma_pairs]
    verb_lemmas = [prune_lemmas(verbs, stop_words) for _, verbs in lemma_pairs]
    documents = [
        build_document(nouns, verbs)
        for nouns, verbs in zip(noun_lemmas, verb_lemmas, strict=True)
    ]
    is_empty = [len(document) == 0 for document in documents]

    processed = to_process.with_columns(
        (pl.col("gtl_id").str.slice(0, 2) + "000000").alias("node_id"),
        pl.Series("noun_lemmas", noun_lemmas, dtype=pl.List(pl.String)),
        pl.Series("verb_lemmas", verb_lemmas, dtype=pl.List(pl.String)),
        pl.Series("document", documents, dtype=pl.String),
        pl.Series("is_empty", is_empty, dtype=pl.Boolean),
    )

    result = pl.concat([processed, deleted], how="diagonal_relaxed")
    result.write_parquet(output_file_path)

    log_config_to_run(
        resolved_run_id,
        "vocabulary",
        config.vocabulary,
        str(CONFIGS_PATH / f"{config_file_name}.yaml"),
        metrics={"n_processed": processed.height, "n_deleted": deleted.height},
    )
    logger.info(
        f"Processed {processed.height} items ({deleted.height} deleted) "
        f"-> {output_file_path}"
    )


if __name__ == "__main__":
    app()
