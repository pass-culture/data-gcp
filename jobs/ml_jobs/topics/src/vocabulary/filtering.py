"""Step 1 of the vocabulary pipeline: document filtering (spec section 7.1).

Language detection runs on *distinct* descriptions (many items share an
identical description, e.g. reprints) and is parallelized above
``LANGUAGE_DETECTION_PARALLEL_THRESHOLD`` texts, per the spec's guidance that
py3langid is fast and deterministic but still benefits from multiple
processes at scale.
"""

from concurrent.futures import ProcessPoolExecutor

import polars as pl
import py3langid as langid

KEPT_LANGUAGE = "fr"
LANGUAGE_DETECTION_PARALLEL_THRESHOLD = 50_000
LANGUAGE_DETECTION_CHUNK_SIZE = 5_000


def filter_book_documents(items: pl.DataFrame) -> pl.DataFrame:
    """Steps 1.1-1.3: BOOK rows with a GTL id and a level-1 label, trimmed and
    deduplicated non-empty descriptions.
    """
    return (
        items.filter(
            (pl.col("gtl_type") == "BOOK")
            & pl.col("gtl_id").is_not_null()
            & pl.col("gtl_label_level_1").is_not_null()
            & (pl.col("gtl_label_level_1") != "")
        )
        .with_columns(pl.col("offer_description").str.strip_chars())
        .filter(pl.col("offer_description") != "")
        .unique(subset="item_id", keep="first")
    )


def _classify_language(text: str) -> str:
    return langid.classify(text)[0]


def detect_languages(texts: list[str]) -> list[str]:
    """One language code per text, parallelized above
    ``LANGUAGE_DETECTION_PARALLEL_THRESHOLD`` texts.
    """
    if len(texts) <= LANGUAGE_DETECTION_PARALLEL_THRESHOLD:
        return [_classify_language(text) for text in texts]

    with ProcessPoolExecutor() as executor:
        return list(
            executor.map(
                _classify_language, texts, chunksize=LANGUAGE_DETECTION_CHUNK_SIZE
            )
        )


def filter_french(items: pl.DataFrame) -> pl.DataFrame:
    """Step 1.4: detect language on distinct descriptions, keep ``lang == "fr"``."""
    distinct_descriptions = items.select("offer_description").unique()
    languages = detect_languages(distinct_descriptions["offer_description"].to_list())
    lang_by_description = distinct_descriptions.with_columns(
        pl.Series("lang", languages)
    )
    return items.join(lang_by_description, on="offer_description", how="left").filter(
        pl.col("lang") == KEPT_LANGUAGE
    )


def filter_documents(items: pl.DataFrame) -> pl.DataFrame:
    """Step 1, in full: BOOK/GTL filtering, trimming, deduplication, then
    French-language filtering.
    """
    return filter_french(filter_book_documents(items))
