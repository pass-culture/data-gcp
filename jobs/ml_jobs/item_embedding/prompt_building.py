"""Turning (already-preprocessed) feature columns into the text prompt sent to
an encoder: either a natural-language ``prompt_template`` or the default
``"label : value"`` concatenation.

This is the logic behind the ``build_prompts`` step (``cli/build_prompts.py``).
It assumes preprocessing has already run (``cli/preprocess.py``) and knows
nothing about encoders.
"""

import pandas as pd
from config import Vector
from loguru import logger
from preprocessing import _is_missing


def _build_prompts_from_template(df: pd.DataFrame, vector: Vector) -> list[str]:
    """Build prompts by rendering ``vector.prompt_template`` per row.

    Null feature values render as ``""`` (not the literal ``"None"``). Rows
    where every declared feature is null get an empty prompt, kept in place.

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

    return rendered.tolist()


def build_prompts(df: pd.DataFrame, vector: Vector) -> list[str]:
    """Build one text prompt per row.

    If ``vector.prompt_template`` is set, renders that template per row.
    Otherwise concatenates non-null feature values as ``"label : value"`` pairs
    separated by newlines (label defaults to the column name unless overridden
    in ``vector.labels``). Items with all-null features get an empty prompt
    (kept in place, row-aligned with ``df``) and are logged.

    Args:
        df: DataFrame with the (preprocessed) feature columns.
        vector: Vector configuration.

    Returns:
        List of prompt strings, one per row (empty string for all-null rows).
    """
    if vector.prompt_template is not None:
        prompts = _build_prompts_from_template(df, vector)
    else:
        parts = []
        for feature in vector.features:
            label = vector.labels.get(feature, feature)
            mask = df[feature].notna() & (df[feature].astype(str).str.strip() != "")
            formatted = pd.Series("", index=df.index)
            formatted[mask] = label + " : " + df[feature][mask].astype(str)
            parts.append(formatted)

        # Join non-null parts per row with a newline (null features omitted).
        prompts = (
            pd.concat(parts, axis=1)
            .agg(lambda row: "\n".join(filter(None, row)), axis=1)
            .tolist()
        )

    empty_count = sum(1 for prompt in prompts if prompt == "")
    if empty_count:
        logger.warning(
            f"Vector '{vector.name}': {empty_count} row(s) have all-null "
            f"features and produced an empty prompt."
        )

    return prompts
