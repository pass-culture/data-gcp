"""Feature preprocessing: reusable column-level cleaners plus the helper that
applies a vector's configured preprocessors to a dataframe.

This is part of the logic behind the ``prepare`` step (``cli/prepare.py``). It
deliberately knows nothing about prompts or encoders -- it only turns raw
feature columns into cleaned ones.
"""

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


# Registry of named preprocessors referenceable from a vector's YAML config.
PREPROCESSORS: dict[str, Callable[[Any], Optional[str]]] = {
    "normalize_whitespace": normalize_whitespace,
    "clean_description": clean_description,
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
