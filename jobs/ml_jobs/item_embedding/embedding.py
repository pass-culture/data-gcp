import numpy as np
import pandas as pd
from config import CategoryFilter, FilterCondition, Vector
from constants import BATCH_SIZE
from loguru import logger
from preprocessing import PREPROCESSORS
from sentence_transformers import SentenceTransformer


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
        working[feature] = working[feature].map(lambda v: fn(v) if pd.notna(v) else v)
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
            feature: (row[feature] if pd.notna(row[feature]) else "")
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

        vector_embeddings = embed_vector(
            vector,
            encoders[vector.encoder_name],
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

    Returns:
        DataFrame with 'item_id', 'content_hash', and one column per vector.
        Each vector column contains an embedding array for rows it applies
        to (missing/NaN for items outside a scoped vector's category, or
        that vector's own empty-prompt rows).
    """
    pools = pools or {}
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
        df, global_vectors, encoders, pools, base_columns=["item_id", "content_hash"]
    )
    result = result.merge(
        global_result.drop(columns=["content_hash"]), on="item_id", how="left"
    )

    for vector in scoped_vectors:
        subset = df[_category_filter_mask(df, vector.category_filter)]
        scoped_result = _embed_vector_group(
            subset, [vector], encoders, pools, base_columns=["item_id"]
        )
        result = result.merge(scoped_result, on="item_id", how="left")

    vector_names = [vector.name for vector in vectors]
    if vector_names:
        result = result[result[vector_names].notna().any(axis=1)]

    return result[["item_id", "content_hash"] + vector_names].reset_index(drop=True)
