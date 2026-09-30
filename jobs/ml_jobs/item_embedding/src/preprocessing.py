"""Feature preprocessing: reusable column-level cleaners plus the helper that
applies a vector's configured preprocessors to a dataframe.

This is part of the logic behind the ``prepare`` step (``cli/prepare.py``). It
deliberately knows nothing about prompts or encoders -- it only turns raw
feature columns into cleaned ones.
"""

import json
import re
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd


def _is_missing(value: object) -> bool:
    """True if ``value`` should be treated as missing.

    Unlike ``pd.notna``, this is safe on list/dict values: ``pd.notna`` on a
    list vectorizes elementwise and returns an array (raising when used as a
    plain bool). Only ``None`` and float ``NaN`` are treated as missing; any
    other value (including lists/dicts) is considered present.
    """
    if value is None:
        return True
    if isinstance(value, float):
        return bool(np.isnan(value))
    return False


def normalize_whitespace(value: Optional[str]) -> Optional[str]:
    """Whitespace preprocessor: collapses runs of whitespace and strips ends.

    Real title cleaning logic (accents, punctuation, casing, boilerplate
    removal, etc.) is to be detailed later. ``None`` passes through unchanged
    so callers don't need to null-check before applying a preprocessor.
    """
    if value is None:
        return value
    return " ".join(str(value).split())


_BOILERPLATE_PHRASES = [
    "Tous les détails du film sur AlloCiné:",
    "Pour plus d informations, rendez-vous sur",
    "Pour plus d'informations, rendez-vous sur",
]
_URL_OR_BOILERPLATE_PATTERN = re.compile(
    r"https?://\S+|www\.\S+|"
    + "|".join(re.escape(phrase) for phrase in _BOILERPLATE_PHRASES)
)


def clean_description(value: Optional[str]) -> Optional[str]:
    """Cleans a description: strips http(s)/www URLs and known boilerplate
    phrases, then applies ``normalize_whitespace`` to collapse all remaining
    whitespace (including line breaks) to single spaces and trim the ends.
    ``None`` passes through unchanged.
    """
    if value is None:
        return value
    text = _URL_OR_BOILERPLATE_PATTERN.sub("", str(value))
    return normalize_whitespace(text)


def _parse_extra_semantic_metadata(value: Any) -> Optional[dict]:
    """Parses the ``extra_semantic_metadata`` envelope, accepting either a
    native dict or a JSON-encoded string. Returns ``None`` for ``None`` input.
    """
    if value is None:
        return None
    return json.loads(value) if isinstance(value, str) else value


def format_movie_genres(value: Any) -> Optional[str]:
    """Formats a movie genre list from a uniform ``extra_semantic_metadata``
    envelope -- ``{"movies": {"genres": ["DRAMA", "ACTION"]}}`` (or the
    JSON-encoded string equivalent) -- as ``"DRAMA, ACTION"``.

    ``"movies"`` is a fixed content-type key, independent of the vector's
    name. Returns ``None`` if the envelope/"movies"/"genres" is absent/empty.
    """
    envelope = _parse_extra_semantic_metadata(value)
    if envelope is None:
        return None
    genres = (envelope.get("movies") or {}).get("genres")
    if not genres:
        return None
    return ", ".join(str(genre) for genre in genres)


_GTL_LEVEL_LABELS = {
    "gtl1": "niveau 1",
    "gtl2": "niveau 2",
    "gtl3": "niveau 3",
    "gtl4": "niveau 4",
}


def format_book_classification(value: Any) -> Optional[str]:
    """Formats the hierarchical GTL classification from a uniform
    ``extra_semantic_metadata`` envelope -- ``{"books": {"gtl1": "roman",
    "gtl2": "19eme siecle", ...}}`` -- as a labeled chevron chain:
    ``"niveau 1 : roman > niveau 2 : 19eme siecle"``.

    GTL has no fixed per-level meaning, so each value is tagged by its raw
    level position. Missing levels are skipped. Returns ``None`` if no level
    is populated.
    """
    envelope = _parse_extra_semantic_metadata(value)
    if envelope is None:
        return None
    classification = envelope.get("books") or {}
    parts = [
        f"{_GTL_LEVEL_LABELS[key]} : {classification[key]}"
        for key in _GTL_LEVEL_LABELS
        if classification.get(key)
    ]
    if not parts:
        return None
    return " > ".join(parts)


# Registry of named preprocessors referenceable from a vector's YAML config.
PREPROCESSORS: dict[str, Callable[[Any], Optional[str]]] = {
    "normalize_whitespace": normalize_whitespace,
    "clean_description": clean_description,
    "format_movie_genres": format_movie_genres,
    "format_book_classification": format_book_classification,
}


def apply_preprocessors(
    df: pd.DataFrame, preprocessors: dict[str, str]
) -> pd.DataFrame:
    """Return a copy of ``df`` with each configured preprocessor applied to its
    column. Missing values are left untouched (the preprocessor is not called),
    so a function never has to null-check its input.

    Args:
        df: DataFrame with the feature columns.
        preprocessors: Mapping of column name -> registered preprocessor name
            (typically ``vector.preprocessors``). Empty mapping is a no-op copy.
    """
    working = df.copy()
    for feature, preprocessor_name in preprocessors.items():
        fn = PREPROCESSORS[preprocessor_name]
        working[feature] = working[feature].map(
            lambda value, fn=fn: value if _is_missing(value) else fn(value)
        )
    return working
