import json
import re
from typing import Any, Callable, Optional


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


def _parse_extra_semantic_metadata(value: Any) -> Optional[dict]:
    """Parses the ``extra_semantic_metadata`` envelope, accepting either a
    native dict or a JSON-encoded string (the more likely production shape,
    since BigQuery can't natively hold a different type per row in one
    column -- see ``format_movie_genres``/``format_book_classification``).
    Returns `None` for `None` input.
    """
    if value is None:
        return None
    return json.loads(value) if isinstance(value, str) else value


def format_movie_genres(value: Any) -> Optional[str]:
    """Formats the movie genre list from a uniform ``extra_semantic_metadata``
    envelope -- ``{"movies": {"genres": ["DRAMA", "ACTION"]}}`` (or the
    JSON-encoded string equivalent) -- as a comma-separated string:
    ``"DRAMA, ACTION"``.

    ``"movies"`` is a fixed content-type key, independent of whatever name
    the movies-scoped vector happens to be given in config, so renaming that
    vector doesn't silently break metadata extraction.

    Returns `None` if the envelope, the "movies" entry, or its "genres" list
    is absent/empty, so the field renders blank in a template instead of an
    artifact like an empty pair of brackets.
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
    "gtl2": "19eme siecle", "gtl3": "tragedie", "gtl4": None}}`` (or the
    JSON-encoded string equivalent) -- as a labeled chevron chain:
    ``"niveau 1 : roman > niveau 2 : 19eme siecle > niveau 3 : tragedie"``.

    ``"books"`` is a fixed content-type key, independent of whatever name
    the books-scoped vector happens to be given in config, so renaming that
    vector doesn't silently break metadata extraction.

    GTL (`gtl1`..`gtl4`, matching the `gtl_label_level_1`..`_4` columns used
    elsewhere in the codebase) is a generic 4-level hierarchical
    classification with no fixed semantic meaning per level (it isn't always
    genre > sub-genre > type; the taxonomy can stop at any depth), so each
    value is tagged with its raw level position rather than an invented
    category name. The ``>`` separator (a common breadcrumb/taxonomy
    convention, e.g. product category paths) reinforces the parent-to-child
    ordering on top of the explicit level tag, more compactly than a
    comma-separated list. Together they disambiguate a term appearing at
    different depths — e.g. "tragedie" reads as "niveau 3 : tragedie" for a
    book classified roman > 19eme siecle > tragedie, but as "niveau 2 :
    tragedie" for one classified theatre > tragedie > grecque.

    Missing/`None` levels (and a missing/`None` "books" entry) are skipped.
    Returns `None` if no level is populated.
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


PREPROCESSORS: dict[str, Callable[[Any], Optional[str]]] = {
    "normalize_whitespace": normalize_whitespace,
    "clean_description": clean_description,
    "format_movie_genres": format_movie_genres,
    "format_book_classification": format_book_classification,
}
