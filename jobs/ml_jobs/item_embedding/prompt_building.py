"""Everything involved in turning item metadata into the text prompts sent
to an encoder: default/template rendering, preprocessing, category-filter
row selection, and detecting prompts that will be silently truncated.

Kept separate from ``embedding.py``, which owns dispatching those prompts to
the encoder(s) and assembling the final embeddings dataframe.
"""

from dataclasses import dataclass, field

import numpy as np
import pandas as pd
from config import CategoryFilter, FilterCondition, Vector
from constants import MAX_SEQ_LENGTH
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

    max_tokens: int = MAX_SEQ_LENGTH
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
