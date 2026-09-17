from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
from config import Vector
from constants import BATCH_SIZE
from loguru import logger
from prompt_building import (
    LongPromptTracker,
    _build_prompts,
    _category_filter_mask,
    _find_long_prompts,
)
from sentence_transformers import SentenceTransformer


def embed_vector(
    label: str,
    encoder: SentenceTransformer,
    prompts: list[str],
    prompt_name: Optional[str] = None,
    pool: object = None,
) -> np.ndarray:
    """Generate embeddings for a batch of prompts.

    The batch may be a single vector's prompts, or several vectors' prompts
    merged together by ``_batch_encode`` (they share an encoder and
    prompt_name, so one encode() call is equivalent to -- but far cheaper
    than -- issuing one per vector). Uses multi-GPU encoding when a
    pre-started ``pool`` is provided, otherwise falls back to single-device
    encoding.

    Args:
        label: Human-readable label for log lines (e.g. a vector name, or
            several joined together when prompts were merged).
        encoder: Pre-loaded SentenceTransformer encoder
        prompts: Pre-built prompts
        prompt_name: The encoder's named prompt to prepend (shared by every
            vector contributing to this batch; see ``_batch_encode``).
        pool: Pre-started multi-process pool, or ``None`` for single-device

    Returns:
        Numpy array of shape (n_items, embedding_dim)
    """
    logger.info(f"Processing '{label}' (batch_size={BATCH_SIZE})")
    encode_kwargs = {
        "convert_to_numpy": True,
        "show_progress_bar": False,
        "batch_size": BATCH_SIZE,
        "prompt_name": prompt_name,
        "normalize_embeddings": True,
    }

    if pool is not None:
        logger.info(f"Using multi-GPU encoding for {len(prompts)} prompts")
        embeddings = encoder.encode(prompts, pool=pool, **encode_kwargs)
    else:
        logger.info(f"Using single-device encoding ({encoder.device})")
        embeddings = encoder.encode(prompts, **encode_kwargs)

    logger.info(f"Generated {len(embeddings)} embeddings with shape {embeddings.shape}")
    return embeddings


@dataclass
class _PendingVectorEmbed:
    """One vector's prompts, prepared and ready to be merged into a batched
    ``encode()`` call by ``_batch_encode``.
    """

    vector: Vector
    prompts: list[str]


def _build_vector_prompts(
    df: pd.DataFrame,
    vectors: list[Vector],
    encoders: dict[str, SentenceTransformer],
    base_columns: list[str],
    tracker: LongPromptTracker,
) -> tuple[pd.DataFrame, list[_PendingVectorEmbed]]:
    """Builds prompts and applies row-filtering for a group of vectors
    sharing the same item set, stopping short of calling the encoder so the
    actual GPU dispatch can be batched across groups by ``_batch_encode``.

    Items with no metadata to embed for one or more of ``vectors`` (all-null
    features, i.e. an empty prompt) are skipped entirely and logged, so every
    vector's prompts are aligned with the identity frame's surviving rows.
    With an empty ``vectors`` list, every row of ``df`` passes through
    unchanged (the AND-check over zero columns is trivially true).

    Args:
        df: DataFrame with item metadata for this group's item set.
        vectors: Vector configurations sharing this item set.
        encoders: Pre-loaded encoders keyed by encoder name (used only to
            run the token-length check, not to encode).
        base_columns: Identifier columns to carry through (must include
            "item_id"; "content_hash" is only needed on the caller's global
            call, since the final identity frame owns it otherwise).
        tracker: Accumulates items whose prompt exceeds the encoder's token
            limit (see ``_find_long_prompts``).

    Returns:
        Tuple of (identity_df, pending):
        - identity_df: ``base_columns`` for the surviving rows only.
        - pending: one ``_PendingVectorEmbed`` per vector in ``vectors``,
          whose ``prompts`` are row-aligned with identity_df (empty when no
          row survives).
    """
    prompts_df = df[base_columns].reset_index(drop=True)
    for vector in vectors:
        prompts_df[vector.name] = _build_prompts(df, vector)

    vector_names = [vector.name for vector in vectors]
    complete = (prompts_df[vector_names] != "").all(axis=1)

    dropped_items = prompts_df.loc[~complete, "item_id"].tolist()
    if dropped_items:
        logger.warning(
            f"Skipping {len(dropped_items)} item(s) with no metadata to embed; "
            f"they are excluded from the output. Item ids:\n{dropped_items}"
        )
    if vector_names and not complete.any():
        logger.warning("No item has metadata to embed; returning an empty result.")

    prompts_df = prompts_df[complete].reset_index(drop=True)

    identity_df = prompts_df[base_columns].copy()
    pending = []
    for vector in vectors:
        item_ids = prompts_df["item_id"].tolist()
        prompts = prompts_df[vector.name].tolist()
        if prompts:
            _find_long_prompts(
                vector, encoders[vector.encoder_name], item_ids, prompts, tracker
            )
        pending.append(_PendingVectorEmbed(vector=vector, prompts=prompts))

    return identity_df, pending


def _batch_encode(
    pending: list[_PendingVectorEmbed],
    encoders: dict[str, SentenceTransformer],
    pools: dict[str, object],
) -> dict[str, np.ndarray]:
    """Groups ``pending`` entries by ``(encoder_name, prompt_name)`` and
    issues one ``encoder.encode()`` call per group, merging every group
    member's prompts into a single call.

    This is the fix for GPU underutilization when a category-scoped vector's
    subset is small relative to a sibling vector sharing the same encoder
    (e.g. ``movies_content`` vs. ``books_content`` in
    configs/category_embeddings.yaml): instead of one near-idle multi-GPU
    dispatch for a couple hundred prompts and a separate one for tens of
    thousands, both ride in a single dispatch. Grouping is keyed on
    ``prompt_name`` too (not just ``encoder_name``) because it selects a
    fixed prefix string that ``encode()`` applies to every prompt in the
    call -- merging vectors with different prompt_names would silently
    corrupt one of them.

    Entries with no prompts (e.g. a scoped vector whose category is absent
    from this chunk) contribute nothing and are skipped before grouping, so
    they never trigger an encode() call, matching the previous per-vector
    short-circuit. A group with a single member reuses its prompt list
    as-is (no concatenation copy), so a config with no shared
    ``(encoder_name, prompt_name)`` pairs -- e.g. default.yaml's single,
    unfiltered vector -- does zero additional per-item work versus before.

    Args:
        pending: Vectors' prepared prompts to encode, flattened across every
            vector group in the current chunk (global vectors plus every
            scoped vector), from one or more prior calls to
            ``_build_vector_prompts``.
        encoders: Pre-loaded encoders keyed by encoder name.
        pools: Pre-started multi-process pools keyed by encoder name.

    Returns:
        Mapping of vector.name -> embeddings array, row-aligned with that
        vector's ``prompts`` in ``pending``. A vector with no prompts is
        absent from the returned dict.
    """
    groups: dict[tuple[str, Optional[str]], list[_PendingVectorEmbed]] = {}
    for entry in pending:
        if not entry.prompts:
            continue
        key = (entry.vector.encoder_name, entry.vector.prompt_name)
        groups.setdefault(key, []).append(entry)

    results: dict[str, np.ndarray] = {}
    for (encoder_name, prompt_name), members in groups.items():
        encoder = encoders[encoder_name]
        merged_prompts = (
            members[0].prompts
            if len(members) == 1
            else [p for member in members for p in member.prompts]
        )
        label = "+".join(member.vector.name for member in members)

        embeddings = embed_vector(
            label,
            encoder,
            prompts=merged_prompts,
            prompt_name=prompt_name,
            pool=pools.get(encoder_name),
        )

        offset = 0
        for member in members:
            n = len(member.prompts)
            results[member.vector.name] = embeddings[offset : offset + n]
            offset += n

    return results


def embed_dataframe(
    df: pd.DataFrame,
    vectors: list[Vector],
    encoders: dict[str, SentenceTransformer],
    pools: dict[str, object] = None,
    tracker: Optional[LongPromptTracker] = None,
) -> pd.DataFrame:
    """Compute all vector embeddings for a dataframe.

    Vectors without a ``category_filter`` ("global" vectors) keep the
    original all-or-nothing behavior: an item must have metadata for *every*
    global vector to appear in the output, computed exactly as before.

    Vectors with a ``category_filter`` ("scoped" vectors, e.g. a
    movies-only or books-only vector) are restricted to the subset of items
    matching their filter and are embedded independently of every other
    vector, so an item outside a vector's category simply gets a missing
    value for that vector's column rather than being excluded from the whole
    output.

    An item is included in the output if it has a value for at least one
    configured vector (global or scoped). This is a no-op generalization of
    the original rule for global-only configs (a row only ever has partial
    global values if it already failed the all-or-nothing check, in which
    case it has none), and is what keeps a category-only config (e.g. movies
    + books, no global vectors) scoped to actual movies/books instead of the
    entire input DataFrame with all-null columns.

    Args:
        df: DataFrame with item metadata (must contain 'item_id', 'content_hash'
            and all feature columns required by the vectors in the config file)
        vectors: Vector configurations
        encoders: Pre-loaded encoders keyed by encoder name
        pools: Pre-started multi-process pools keyed by encoder name. When a
            pool exists for a vector's encoder it is reused; otherwise the
            vector is encoded on a single device.
        tracker: Accumulates items whose prompt exceeds the encoder's token
            limit (silently truncated otherwise). Pass the same tracker
            across multiple ``embed_dataframe`` calls (e.g. once per input
            parquet file) to get one aggregated end-of-job summary via
            ``tracker.log_summary()``; defaults to a fresh, unreported
            tracker when omitted.

    Returns:
        DataFrame with 'item_id', 'content_hash', and one column per vector.
        Each vector column contains an embedding array for rows it applies
        to (missing/NaN for items outside a scoped vector's category, or
        that vector's own empty-prompt rows).
    """
    pools = pools or {}
    tracker = tracker if tracker is not None else LongPromptTracker()
    logger.info(f"Embedding {len(df)} items")

    if df["item_id"].duplicated().any():
        raise ValueError("Input dataframe has duplicate item_id values")

    global_vectors = [vector for vector in vectors if vector.category_filter is None]
    scoped_vectors = [
        vector for vector in vectors if vector.category_filter is not None
    ]

    # Identity frame carries every item's item_id/content_hash regardless of
    # which vector(s) end up populated for it, so content_hash is always
    # correct even for items only reached by a scoped vector.
    result = df[["item_id", "content_hash"]].copy()

    global_identity, global_pending = _build_vector_prompts(
        df,
        global_vectors,
        encoders,
        base_columns=["item_id", "content_hash"],
        tracker=tracker,
    )

    scoped_prepared = []
    for vector in scoped_vectors:
        subset = df[_category_filter_mask(df, vector.category_filter)]
        scoped_identity, scoped_pending = _build_vector_prompts(
            subset, [vector], encoders, base_columns=["item_id"], tracker=tracker
        )
        scoped_prepared.append((scoped_identity, scoped_pending))

    # Batching across every vector prepared above -- global and scoped alike
    # -- is what lets a small scoped vector's prompts (e.g. movies_content)
    # ride inside the same encode() dispatch as a sibling sharing its
    # (encoder_name, prompt_name) (e.g. books_content), instead of paying
    # multi-GPU dispatch overhead on its own for a near-empty batch.
    all_pending = global_pending + [
        entry for _, pending in scoped_prepared for entry in pending
    ]
    embeddings_by_vector_name = _batch_encode(all_pending, encoders, pools)

    for vector in global_vectors:
        embeddings = embeddings_by_vector_name.get(vector.name)
        global_identity[vector.name] = (
            embeddings.tolist() if embeddings is not None else pd.Series(dtype=object)
        )
    result = result.merge(
        global_identity.drop(columns=["content_hash"]), on="item_id", how="left"
    )

    for scoped_identity, pending in scoped_prepared:
        vector = pending[0].vector
        embeddings = embeddings_by_vector_name.get(vector.name)
        scoped_identity[vector.name] = (
            embeddings.tolist() if embeddings is not None else pd.Series(dtype=object)
        )
        result = result.merge(scoped_identity, on="item_id", how="left")

    vector_names = [vector.name for vector in vectors]
    if vector_names:
        result = result[result[vector_names].notna().any(axis=1)]

    return result[["item_id", "content_hash"] + vector_names].reset_index(drop=True)
