"""Decide a safe ``max_seq_length`` truncation cap for item embeddings.

Runs two analyses on a BigQuery sample of the embedding input table, building
prompts with the exact same logic as the job (:func:`embedding._build_prompts`)
and tokenizing with the real encoder tokenizer (prompt prefix included):

1. Length distribution: token-length percentiles per vector and the share of the
   catalogue that would be truncated at each candidate cap. Answers "how many
   items are even affected?".
2. Quality drift: embeds the longest items at full length vs each cap and reports
   cosine similarity on the truncated population only. Answers "how much does
   truncation move the vector?".

Usage (from this directory):
    uv run python analyze_truncation.py --embed-sample 2000 --caps 256,384,512,768
"""

import re

import numpy as np
import pandas as pd
import torch
import typer
from config import Vector, parse_vectors
from constants import GCP_PROJECT_ID, HF_TOKEN_SECRET_NAME
from embedding import _build_prompts
from gcp_secrets import get_secret
from google.cloud import bigquery
from loguru import logger
from sentence_transformers import SentenceTransformer

app = typer.Typer(help="Analyze how much item-embedding prompts need truncation.")

# BigQuery table/column identifiers are interpolated into SQL, so restrict them
# to safe characters to avoid injection from CLI/config input.
_TABLE_RE = re.compile(r"^[A-Za-z0-9_\-.:]+$")
_COLUMN_RE = re.compile(r"^[A-Za-z_]\w*$", re.ASCII)

_PERCENTILES = [50, 90, 95, 99, 99.9, 100]


def _default_table() -> str:
    return f"{GCP_PROJECT_ID}.ml_input_stg.item_embedding_extraction"


def _fetch_sample(table: str, features: list[str], sample_size: int) -> pd.DataFrame:
    """Fetch a random sample of the required feature columns from BigQuery."""
    if not _TABLE_RE.match(table):
        raise ValueError(f"Unsafe table identifier: {table!r}")
    columns = ["item_id", "content_hash", *features]
    for column in columns:
        if not _COLUMN_RE.match(column):
            raise ValueError(f"Unsafe column identifier: {column!r}")

    select_list = ", ".join(f"`{column}`" for column in columns)
    query = (
        f"SELECT {select_list} FROM `{table}` "
        f"ORDER BY RAND() LIMIT {int(sample_size)}"
    )
    logger.info(f"Querying {int(sample_size)} rows from {table}")
    client = bigquery.Client(project=GCP_PROJECT_ID)
    df = client.query(query).to_dataframe()
    logger.info(f"Fetched {len(df)} rows")
    return df


def _prompt_prefix(model: SentenceTransformer, prompt_name: str | None) -> str:
    """Return the prompt-template string the model prepends for ``prompt_name``."""
    if not prompt_name:
        return ""
    return getattr(model, "prompts", {}).get(prompt_name, "")


def _token_lengths(
    model: SentenceTransformer, prompts: list[str], prefix: str
) -> np.ndarray:
    """Token count each model sees per prompt (prefix + content + special tokens)."""
    tokenizer = model.tokenizer
    texts = [prefix + prompt for prompt in prompts]
    encoded = tokenizer(
        texts, add_special_tokens=True, truncation=False, padding=False
    )["input_ids"]
    return np.array([len(ids) for ids in encoded], dtype=np.int64)


def _report_distribution(lengths: np.ndarray, caps: list[int]) -> None:
    """Print token-length percentiles and truncation share per candidate cap."""
    logger.info("Token-length distribution (tokens the model actually sees):")
    for percentile in _PERCENTILES:
        logger.info(f"  p{percentile:<5} = {np.percentile(lengths, percentile):8.1f}")
    logger.info(f"  mean   = {lengths.mean():8.1f}   max = {lengths.max()}")

    total = len(lengths)
    logger.info("Share of catalogue truncated at each candidate cap:")
    for cap in caps:
        truncated = int((lengths > cap).sum())
        logger.info(
            f"  cap={cap:<5} -> {truncated:>8} / {total} items truncated "
            f"({100 * truncated / total:5.2f}%)"
        )


def _cosine(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Row-wise cosine similarity between two L2-normalizable embedding matrices."""
    a_norm = a / np.linalg.norm(a, axis=1, keepdims=True)
    b_norm = b / np.linalg.norm(b, axis=1, keepdims=True)
    return (a_norm * b_norm).sum(axis=1)


def _report_drift(
    vector: Vector,
    model: SentenceTransformer,
    prompts: list[str],
    lengths: np.ndarray,
    caps: list[int],
    embed_sample: int,
    batch_size: int,
) -> None:
    """Embed the longest items at full length vs each cap and report cosine drift."""
    order = np.argsort(lengths)[::-1]
    sample_idx = order[: min(embed_sample, len(order))]
    sample_prompts = [prompts[i] for i in sample_idx]
    sample_lengths = lengths[sample_idx]

    if sample_lengths.max() <= min(caps):
        logger.info(
            "No sampled item exceeds the smallest cap; truncation is a no-op here."
        )
        return

    encode_kwargs = {
        "convert_to_numpy": True,
        "normalize_embeddings": True,
        "batch_size": batch_size,
        "prompt_name": vector.prompt_name,
        "show_progress_bar": False,
    }

    model.max_seq_length = 2048
    logger.info(
        f"Embedding {len(sample_prompts)} longest items at full length "
        f"(max_seq_length={model.max_seq_length}) as baseline"
    )
    full = model.encode(sample_prompts, **encode_kwargs)

    logger.info("Cosine drift vs full-length baseline, on the TRUNCATED items only:")
    for cap in caps:
        affected = sample_lengths > cap
        n_affected = int(affected.sum())
        if n_affected == 0:
            logger.info(f"  cap={cap:<5} -> no sampled item truncated")
            continue
        model.max_seq_length = cap
        capped = model.encode(sample_prompts, **encode_kwargs)
        sims = _cosine(full[affected], capped[affected])
        logger.info(
            f"  cap={cap:<5} -> {n_affected:>6} truncated | "
            f"cosine min={sims.min():.4f} p1={np.percentile(sims, 1):.4f} "
            f"p5={np.percentile(sims, 5):.4f} median={np.median(sims):.4f} "
            f"mean={sims.mean():.4f}"
        )


@app.command()
def main(
    config_file_name: str = typer.Option("default", help="Config file (no .yaml)."),
    table: str = typer.Option(
        None, help="BigQuery input table. Defaults to the env input table."
    ),
    sample_size: int = typer.Option(
        50000, help="Rows to sample for the length distribution."
    ),
    embed_sample: int = typer.Option(
        2000, help="Longest items to embed for the cosine-drift analysis."
    ),
    caps: str = typer.Option("256,384,512,768", help="Comma-separated candidate caps."),
    batch_size: int = typer.Option(32, help="Batch size for the drift embedding."),
) -> None:
    """Run the length-distribution and cosine-drift analyses per vector."""
    table = table or _default_table()
    logger.info(f"Using table={table}")
    candidate_caps = sorted(int(c) for c in caps.split(",") if c.strip())
    vectors = parse_vectors(config_file_name)
    features = sorted({feature for vector in vectors for feature in vector.features})

    df = _fetch_sample(table, features, sample_size)

    token = get_secret(HF_TOKEN_SECRET_NAME)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    dtype = torch.float32  # float16 makes Gemma overflow; bf16 unneeded for analysis
    logger.info(f"Running analysis on device={device}")

    for vector in vectors:
        logger.info(f"{'=' * 70}\nVECTOR: {vector.name} ({vector.encoder_name})")
        prompts = _build_prompts(df, vector)

        model = SentenceTransformer(
            vector.encoder_name,
            token=token,
            device=device,
            model_kwargs={"torch_dtype": dtype},
        )
        prefix = _prompt_prefix(model, vector.prompt_name)
        logger.info(
            f"Prompt prefix for prompt_name={vector.prompt_name!r}: {prefix!r} "
            f"({_token_lengths(model, [''], prefix)[0]} tokens)"
        )

        lengths = _token_lengths(model, prompts, prefix)
        _report_distribution(lengths, candidate_caps)
        _report_drift(
            vector, model, prompts, lengths, candidate_caps, embed_sample, batch_size
        )


if __name__ == "__main__":
    app()
