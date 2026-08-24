import re
from typing import Callable, Optional


def normalize_whitespace(value: Optional[str]) -> Optional[str]:
    """Whitespace preprocessor: collapses runs of whitespace and strips ends.

    Real title cleaning logic (accents, punctuation, casing, boilerplate
    removal, etc.) is to be detailed later. `None` passes through unchanged
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
    phrases (e.g. "Tous les détails du film sur AlloCiné:", "Pour plus
    d'informations, rendez-vous sur"), then applies `normalize_whitespace` to
    collapse all remaining whitespace (including line breaks) to single
    spaces and trim the ends. `None` passes through unchanged.
    """
    if value is None:
        return value
    text = _URL_OR_BOILERPLATE_PATTERN.sub("", str(value))
    return normalize_whitespace(text)


PREPROCESSORS: dict[str, Callable[[Optional[str]], Optional[str]]] = {
    "normalize_whitespace": normalize_whitespace,
    "clean_description": clean_description,
}
