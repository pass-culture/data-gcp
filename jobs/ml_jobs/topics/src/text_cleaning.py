"""
Strip publisher boilerplate from offer descriptions before sampling / NLP.

Some publishers paste the same notice into every description -- e.g. the
Hachette Livre x BnF reprint notice ("Pour plus d'informations, rendez-vous
sur www.hachettebnf.fr", ...) appears in ~15% of French book descriptions.
Left in, it dominates entity counts and would become a topic of its own.

Rather than hardcoding publisher templates, a segment (line or sentence)
counts as boilerplate when it appears verbatim in a small share (`min_share`) of
distinct descriptions: real description sentences almost never repeat that
often, while templates (and their spelling variants) all do. URLs and the
"Date de l'édition originale : ..." header are removed inline.
"""

from __future__ import annotations

import os
import re
from collections import Counter
from typing import TYPE_CHECKING

import polars as pl
from loguru import logger

if TYPE_CHECKING:
    import pandas as pd

# Templates are pasted either one sentence per line or as a single paragraph,
# so descriptions are split into segments on both line and sentence breaks.
SEGMENT_SPLIT = re.compile(r"(\r?\n|(?<=[.!?])\s+)")

# Removed inline wherever they occur: noise for NER and topics alike.
INLINE_NOISE = [
    re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE),
    re.compile(
        r"date de l['\u2019 ]?\s*[ée]dition originale\s*:\s*[\d/-]*", re.IGNORECASE
    ),
]

# Short repeated segments ("Résumé", "Extrait", ...) aren't templates worth
# stripping and may carry meaning; only long ones are candidates.
MIN_SEGMENT_CHARS = 40

# Stripping runs in Polars (Rust regex engine, on Arrow memory): no Python
# loop per text and no pandas -> NumPy conversion, which on 1.7M long texts
# blew up into a 319 GiB fixed-width unicode array. The column is cut into
# this many slices per core, processed in parallel threads.
SLICES_PER_CORE = 2


def segments(text: str) -> list[str]:
    """Alternating [segment, separator, segment, ...], so that joining the
    list gives back the text unchanged."""
    return SEGMENT_SPLIT.split(text)


def find_boilerplate_segments(
    texts: pl.Series, min_share: float = 3e-4, max_texts: int = 200_000, seed: int = 0
) -> list[str]:
    """Segments (stripped, >= MIN_SEGMENT_CHARS) found in >= min_share of
    distinct texts. Templates are frequent by definition, so a random subset
    of max_texts distinct texts is enough to find them. Longest first, so
    that a template wins over any shorter template it contains."""
    distinct = texts.drop_nulls().unique()
    if len(distinct) > max_texts:
        distinct = distinct.sample(max_texts, seed=seed)
    counts = Counter(
        seg
        for text in distinct.to_list()
        for seg in {raw.strip() for raw in segments(text)[::2]}
        if len(seg) >= MIN_SEGMENT_CHARS
    )
    min_count = max(2, round(min_share * len(distinct)))
    found = sorted(
        (seg for seg, n in counts.items() if n >= min_count), key=len, reverse=True
    )
    logger.info(
        f"Found {len(found)} boilerplate segments "
        f"(in >= {min_count}/{len(distinct)} distinct texts)"
    )
    return found


def boilerplate_regex(boilerplate: list[str]) -> str:
    """One case-insensitive regex matching any boilerplate segment anywhere
    in a text -- also when pasted inline after the title with no sentence
    break -- tolerant to whitespace differences, plus INLINE_NOISE. Valid
    for both Python `re` and Rust `regex` (used by Polars)."""
    flexible = [r"\s+".join(map(re.escape, seg.split())) for seg in boilerplate]
    return "(?i)" + "|".join(flexible + [p.pattern for p in INLINE_NOISE])


def strip_boilerplate(texts: pl.Expr, regex: str) -> pl.Expr:
    """Remove every match of `regex`, then tidy the whitespace left behind.
    Texts without any match are returned unchanged (original whitespace and
    line endings included)."""
    cleaned = texts.str.replace_all(regex, " ")
    tidied = (
        cleaned.str.replace_all(r"\s*\n\s*", "\n")
        .str.replace_all(r"[ \t]+", " ")
        .str.strip_chars()
    )
    return pl.when(cleaned != texts).then(tidied).otherwise(texts)


def strip_all(texts: pl.Series, regex: str, n_slices: int | None = None) -> pl.Series:
    """strip_boilerplate over `texts`, cut into `n_slices` slices processed
    in parallel threads (default: SLICES_PER_CORE per core). Order is
    preserved."""
    n_slices = n_slices or SLICES_PER_CORE * (os.cpu_count() or 1)
    size = max(1, -(-len(texts) // n_slices))
    frames = [
        pl.LazyFrame({"t": texts.slice(i, size)}).select(
            strip_boilerplate(pl.col("t"), regex)
        )
        for i in range(0, len(texts), size)
    ]
    if not frames:
        return texts
    return pl.concat([frame["t"] for frame in pl.collect_all(frames)]).rename(
        texts.name
    )


def remove_boilerplate(
    df: pd.DataFrame,
    text_col: str = "offer_description",
    min_chars: int = 50,
) -> pd.DataFrame:
    """Strip boilerplate from `text_col` (raw text kept in `<text_col>_raw`),
    then drop items left with fewer than `min_chars` characters -- e.g. BnF
    reprints whose only real content is the title, already in offer_name."""
    raw = pl.from_pandas(df[text_col])
    cleaned = strip_all(raw, boilerplate_regex(find_boilerplate_segments(raw)))
    changed = int((cleaned != raw).sum())
    keep = (cleaned.str.len_chars() >= min_chars).fill_null(False).to_numpy()

    df = df.copy()
    df[f"{text_col}_raw"] = df[text_col]
    df[text_col] = (
        cleaned.to_pandas(use_pyarrow_extension_array=True)
        .astype("string")
        .set_axis(df.index)
    )
    kept = df[keep]
    logger.info(
        f"Boilerplate stripped from {changed}/{len(df)} descriptions; "
        f"{len(kept)} items kept with >= {min_chars} chars left"
    )
    return kept
