from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
from config import CategoryFilter, FilterCondition, Vector
from constants import BATCH_SIZE, MAX_PROMPT_TOKENS
from loguru import logger
from preprocessing import PREPROCESSORS
from sentence_transformers import SentenceTransformer


@dataclass
class LongPromptTracker:
    """Accumulates items whose prompt exceeded the encoder's token limit
    across an entire job run (all parquet files, all vectors), so they can be
    reported once as a single end-of-job summary instead of scattered across
    many per-file log lines.
    """

    max_tokens: int = MAX_PROMPT_TOKENS
    long_item_ids: dict[str, list] = field(default_factory=dict)

    def record(self, vector_name: str, item_id: object) -> None:
        self.long_item_ids.setdefault(vector_name, []).append(item_id)

    def log_summary(self) -> None:
        if not self.long_item_ids:
            logger.info("No prompts exceeded the token limit.")
            return
        for vector_name, item_ids in self.long_item_ids.items():
            logger.warning(
                f"Vector '{vector_name}': {len(item_ids)} item(s) had a prompt "
                f"exceeding the token limit and were silently truncated by the "
                f"encoder. Item ids:\n{item_ids}"
            )


def _condition_mask(df: pd.DataFrame, condition: FilterCondition) -> pd.Series:
    """Boolean mask selecting the rows a single ``FilterCondition`` matches.

    Exactly one of ``values``/``prefix`` is set on ``condition`` (enforced by
    the model): ``values`` selects rows whose ``column`` value is in that
    list; ``prefix`` selects rows whose ``column`` value (as string) starts
    with that prefix, e.g. ``column="item_id", prefix="product"`` for the SQL
    equivalent ``LEFT(item_id, LEN('product')) = 'product'``.
    """
    column = df[condition.column]
    if condition.values is not None:
        return column.isin(condition.values)
    return column.astype(str).str.startswith(condition.prefix)


def _category_filter_mask(
    df: pd.DataFrame, category_filter: CategoryFilter
) -> pd.Series:
    """Boolean mask selecting the rows a ``category_filter`` scopes to.

    Combines two layers, ANDed together:
    - ``all_of``: every condition must hold (AND). Empty list is trivially
      true, so a filter using only ``any_of`` is unaffected.
    - ``any_of``: at least one group must hold (OR across groups), where each
      group's conditions all hold (AND within the group). Empty list is
      trivially true, so a filter using only ``all_of`` is unaffected.
    """
    mask = pd.Series(True, index=df.index)
    for condition in category_filter.all_of:
        mask &= _condition_mask(df, condition)

    if category_filter.any_of:
        any_of_mask = pd.Series(False, index=df.index)
        for group in category_filter.any_of:
            group_mask = pd.Series(True, index=df.index)
            for condition in group.conditions:
                group_mask &= _condition_mask(df, condition)
            any_of_mask |= group_mask
        mask &= any_of_mask

    return mask


def _is_missing(value: object) -> bool:
    """True if `value` should be treated as missing.

    Unlike ``pd.notna``, this is safe on list/dict values: ``pd.notna`` on a
    list vectorizes elementwise and returns an array (raising when used as a
    plain bool) instead of a single True/False, which breaks on JSON columns
    holding lists (e.g. a movie's genre list). Only ``None`` and float
    ``NaN`` are treated as missing; any other value (including lists/dicts)
    is considered present.
    """
    if value is None:
        return True
    if isinstance(value, float):
        return bool(np.isnan(value))
    return False


def _apply_preprocessors(df: pd.DataFrame, vector: Vector) -> pd.DataFrame:
    """Apply the vector's configured preprocessors to a copy of ``df``.

    No-op (returns ``df`` unchanged, no copy) when the vector has no
    preprocessors configured, so vectors that don't use this feature keep
    operating on the original DataFrame exactly as before.
    """
    if not vector.preprocessors:
        return df

    working = df.copy()
    for feature, preprocessor_name in vector.preprocessors.items():
        fn = PREPROCESSORS[preprocessor_name]
        working[feature] = working[feature].map(
            lambda v: v if _is_missing(v) else fn(v)
        )
    return working


def _build_prompts_from_template(df: pd.DataFrame, vector: Vector) -> list[str]:
    """Build prompts by rendering ``vector.prompt_template`` per row.

    Null feature values are substituted with ``""`` before formatting (rather
    than left as ``None``, which ``str.format`` would render as the literal
    text ``"None"``). Rows where every declared feature is null get an empty
    prompt, kept in place, matching the drop contract used downstream.

    Raises:
        ValueError: If the template references a field not in vector.features.
    """
    template = vector.prompt_template

    def render(row: pd.Series) -> str:
        values = {
            feature: ("" if _is_missing(row[feature]) else row[feature])
            for feature in vector.features
        }
        try:
            return template.format(**values)
        except KeyError as e:
            raise ValueError(
                f"Vector '{vector.name}': prompt_template references unknown "
                f"field {e}; declared features: {vector.features}"
            ) from e

    rendered = df.apply(render, axis=1)

    all_null_mask = df[vector.features].isna().all(axis=1)
    if all_null_mask.any():
        rendered = rendered.copy()
        rendered[all_null_mask] = ""

    empty_items = df.index[rendered == ""]
    if len(empty_items) > 0:
        logger.warning(
            f"Vector '{vector.name}': {len(empty_items)} rows have all-null "
            f"features.\n Empty items are:\n {list(empty_items)}"
        )

    return rendered.tolist()


def _build_prompts(df: pd.DataFrame, vector: Vector) -> list[str]:
    """Build text prompts for all rows.

    If ``vector.prompt_template`` is set, renders that template per row (see
    ``_build_prompts_from_template``). Otherwise falls back to the default
    behavior: concatenates non-null feature values as ``"label : value"``
    pairs separated by newlines, vectorized. The label defaults to the column
    name unless overridden in vector.labels. Features with null values are
    omitted entirely from the prompt string. Items with all-null features get
    an empty prompt string (kept in place so the result stays aligned
    row-for-row with ``df``) and are logged.

    If ``vector.preprocessors`` is set, those preprocessors are applied to the
    relevant feature columns before either path builds the prompt.

    Args:
        df: DataFrame with item metadata
        vector: Vector configuration

    Returns:
        List of formatted prompt strings, one per row (empty string for rows
        whose features are all null).
    """
    working = _apply_preprocessors(df, vector)

    if vector.prompt_template is not None:
        return _build_prompts_from_template(working, vector)

    parts = []
    for feature in vector.features:
        label = vector.labels.get(feature, feature)
        mask = working[feature].notna() & (
            working[feature].astype(str).str.strip() != ""
        )
        formatted = pd.Series("", index=working.index)
        formatted[mask] = label + " : " + working[feature][mask].astype(str)
        parts.append(formatted)

    # Join non-null parts per row with a newline (null features are omitted)
    combined = pd.concat(parts, axis=1).agg(
        lambda row: "\n".join(filter(None, row)), axis=1
    )

    empty_items = working.index[combined == ""]
    if len(empty_items) > 0:
        logger.warning(
            f"Vector '{vector.name}': {len(empty_items)} rows have all-null "
            f"features.\n Empty items are:\n {list(empty_items)}"
        )

    return combined.tolist()


def _resolve_prompt_prefix(encoder: SentenceTransformer, vector: Vector) -> str:
    """Returns the fixed prefix ``encoder.encode()`` prepends for
    ``vector.prompt_name`` (e.g. embeddinggemma's "document" prompt), or
    ``""`` if no prompt_name is configured. Mirrors the resolution
    SentenceTransformer does internally, so the text tokenized to check
    length matches what is actually sent to the model.
    """
    if vector.prompt_name is None:
        return ""
    return encoder.prompts.get(vector.prompt_name, "")


def _find_long_prompts(
    vector: Vector,
    encoder: SentenceTransformer,
    item_ids: list,
    prompts: list[str],
    tracker: LongPromptTracker,
) -> None:
    """Flags prompts likely to exceed the encoder's max sequence length.

    SentenceTransformers silently truncates any prompt longer than
    ``max_seq_length`` (dropping the end, no error/warning of its own), so
    truncation is otherwise invisible. Tokenizing every prompt to get an
    exact length is expensive at catalogue scale, so this first applies a
    cheap character-length pre-filter (character count > 2x the token limit
    -- a generous overestimate, since one token is rarely under ~2
    characters) to shortlist candidates, then tokenizes only those (batched,
    without truncation) to get an exact token count. Prompts confirmed over
    the limit are logged and recorded on ``tracker``.
    """
    encoder_max_seq_length = getattr(encoder, "max_seq_length", None)
    max_tokens = (
        encoder_max_seq_length
        if isinstance(encoder_max_seq_length, int)
        else tracker.max_tokens
    )
    length_threshold = 2 * max_tokens

    candidate_indices = [
        i for i, prompt in enumerate(prompts) if len(prompt) > length_threshold
    ]
    if not candidate_indices:
        return

    prefix = _resolve_prompt_prefix(encoder, vector)
    candidate_texts = [prefix + prompts[i] for i in candidate_indices]
    token_counts = [
        len(input_ids)
        for input_ids in encoder.tokenizer(
            candidate_texts, truncation=False, padding=False
        )["input_ids"]
    ]

    for i, token_count in zip(candidate_indices, token_counts):
        if token_count > max_tokens:
            item_id = item_ids[i]
            logger.warning(
                f"Vector '{vector.name}': item '{item_id}' prompt has "
                f"{token_count} tokens, exceeding max_seq_length={max_tokens}; "
                f"it will be silently truncated by the encoder."
            )
            tracker.record(vector.name, item_id)


def embed_vector(
    vector: Vector,
    encoder: SentenceTransformer,
    prompts: list[str],
    pool: object = None,
) -> np.ndarray:
    """Generate embeddings for a single vector configuration.

    Uses multi-GPU encoding when a pre-started ``pool`` is provided, otherwise
    falls back to single-device encoding.

    Args:
        vector: Vector configuration
        encoder: Pre-loaded SentenceTransformer encoder
        prompts: Pre-built prompts
        pool: Pre-started multi-process pool, or ``None`` for single-device

    Returns:
        Numpy array of shape (n_items, embedding_dim)
    """
    logger.info(f"Processing vector '{vector.name}' (batch_size={BATCH_SIZE})")
    encode_kwargs = {
        "convert_to_numpy": True,
        "show_progress_bar": False,
        "batch_size": BATCH_SIZE,
        "prompt_name": vector.prompt_name,
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


def _embed_vector_group(
    df: pd.DataFrame,
    vectors: list[Vector],
    encoders: dict[str, SentenceTransformer],
    pools: dict[str, object],
    base_columns: list[str],
    tracker: LongPromptTracker,
) -> pd.DataFrame:
    """Compute embeddings for a group of vectors sharing the same item set.

    Items with no metadata to embed for one or more of ``vectors`` (all-null
    features, i.e. an empty prompt) are skipped entirely and logged, so every
    vector column in the returned DataFrame is fully populated with real
    embeddings for the rows it contains (non-null vectors). With an empty
    ``vectors`` list, every row of ``df`` passes through unchanged (the
    AND-check over zero columns is trivially true).

    Args:
        df: DataFrame with item metadata for this group's item set.
        vectors: Vector configurations sharing this item set.
        encoders: Pre-loaded encoders keyed by encoder name.
        pools: Pre-started multi-process pools keyed by encoder name.
        base_columns: Identifier columns to carry through (must include
            "item_id"; "content_hash" is only needed on the caller's global
            call, since the final identity frame owns it otherwise).
        tracker: Accumulates items whose prompt exceeds the encoder's token
            limit (see ``_find_long_prompts``).

    Returns:
        DataFrame with ``base_columns`` and one column per vector in
        ``vectors``.
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

    df_embeddings = prompts_df[base_columns].copy()
    for vector in vectors:
        prompts = prompts_df[vector.name].tolist()
        if not prompts:
            df_embeddings[vector.name] = pd.Series(dtype=object)
            continue

        encoder = encoders[vector.encoder_name]
        _find_long_prompts(
            vector, encoder, prompts_df["item_id"].tolist(), prompts, tracker
        )

        vector_embeddings = embed_vector(
            vector,
            encoder,
            prompts=prompts,
            pool=pools.get(vector.encoder_name),
        )
        df_embeddings[vector.name] = vector_embeddings.tolist()

    return df_embeddings


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

    global_result = _embed_vector_group(
        df,
        global_vectors,
        encoders,
        pools,
        base_columns=["item_id", "content_hash"],
        tracker=tracker,
    )
    result = result.merge(
        global_result.drop(columns=["content_hash"]), on="item_id", how="left"
    )

    for vector in scoped_vectors:
        subset = df[_category_filter_mask(df, vector.category_filter)]
        scoped_result = _embed_vector_group(
            subset, [vector], encoders, pools, base_columns=["item_id"], tracker=tracker
        )
        result = result.merge(scoped_result, on="item_id", how="left")

    vector_names = [vector.name for vector in vectors]
    if vector_names:
        result = result[result[vector_names].notna().any(axis=1)]

    return result[["item_id", "content_hash"] + vector_names].reset_index(drop=True)
