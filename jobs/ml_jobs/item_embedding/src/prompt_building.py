"""Turning (already-preprocessed) feature columns into the text prompt sent to
an encoder: either a natural-language ``prompt_template`` or the default
``"label : value"`` concatenation.

This is part of the logic behind the ``prepare`` step (``cli/prepare.py``),
run after the preprocessors in the same step. It knows nothing about encoders.
"""

from string import Formatter

import pandas as pd
from loguru import logger
from src.config import Vector
from src.preprocessing import _is_missing


def _template_line_fields(line: str) -> list[str]:
    """Field names referenced by the ``{placeholder}``s on one template line."""
    return [name for _, name, _, _ in Formatter().parse(line) if name is not None]


def _build_prompts_from_template(df: pd.DataFrame, vector: Vector) -> list[str]:
    """Build prompts by rendering ``vector.prompt_template`` per row.

    The template is split on newlines and rendered line by line; a line is
    dropped for a row when every ``{placeholder}`` it holds is empty/missing,
    so an optional metadata line (e.g. ``Genres: {movie_genres}``) disappears
    instead of leaving a dangling label. Lines with no placeholders are always
    kept. Surviving lines are joined with a single space, so a row whose every
    feature is null yields an empty prompt. Null values render as ``""`` (not
    the literal ``"None"``).

    Raises:
        ValueError: If the template references a field not in vector.features.
    """
    lines = vector.prompt_template.split("\n")

    def render(row: pd.Series) -> str:
        values = {
            feature: ("" if _is_missing(row[feature]) else row[feature])
            for feature in vector.features
        }
        rendered_lines = []
        for line in lines:
            fields = _template_line_fields(line)
            all_known_and_empty = (
                bool(fields)
                and all(field in values for field in fields)
                and all(values[field] == "" for field in fields)
            )
            if all_known_and_empty:
                continue
            try:
                rendered_lines.append(line.format(**values))
            except KeyError as e:
                raise ValueError(
                    f"Vector '{vector.name}': prompt_template references unknown "
                    f"field {e}; declared features: {vector.features}"
                ) from e
        return " ".join(part for part in rendered_lines if part)

    return df.apply(render, axis=1).tolist()


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
