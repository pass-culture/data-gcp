"""Step 2 of the vocabulary pipeline: description cleaning (spec section
7.1). Boilerplate segments, URLs, and the edition-date header are all folded
into one combined regex (so a template pasted inline after the title is also
caught) and removed in a single Polars ``str.replace_all`` pass -- the Rust
regex engine, not Python's ``re``, is what scales to 1.7M long texts.
"""

import random
import re
from collections import Counter

import polars as pl

URL_PATTERN = r"(?:https?://|www\.)\S+"
# Matches both the straight and the typographic apostrophe seen in real descriptions.
EDITION_HEADER_PATTERN = r"date de l['’ ]?\s*[ée]dition originale\s*:\s*[\d/-]*"  # noqa: RUF001
SEGMENT_SPLIT_PATTERN = r"(?:\r?\n|(?<=[.!?])\s+)"


def find_boilerplate_segments(
    descriptions: list[str],
    *,
    min_segment_chars: int,
    min_share: float,
    max_texts: int,
    seed: int,
) -> list[str]:
    """The distinct, repeated segments to strip as publisher boilerplate.

    Real sentences almost never repeat verbatim across many descriptions;
    templates and their spelling variants do. Sorted longest-first, so a
    template wins over a shorter one it contains when later combined into one
    alternation regex.
    """
    distinct = sorted(set(descriptions))
    if len(distinct) > max_texts:
        distinct = random.Random(seed).sample(distinct, max_texts)

    segment_counts: Counter[str] = Counter()
    for text in distinct:
        segments = {
            segment.strip()
            for segment in re.split(SEGMENT_SPLIT_PATTERN, text)
            if segment and len(segment.strip()) >= min_segment_chars
        }
        segment_counts.update(segments)

    threshold = max(2, round(min_share * len(distinct)))
    boilerplate = [
        segment for segment, count in segment_counts.items() if count >= threshold
    ]
    return sorted(boilerplate, key=len, reverse=True)


def _segment_to_flexible_pattern(segment: str) -> str:
    """A segment as a regex that tolerates any whitespace run where the
    original had one -- so a reflowed/rewrapped copy of the same template
    still matches.
    """
    return r"\s+".join(re.escape(token) for token in segment.split())


def build_boilerplate_pattern(boilerplate_segments: list[str]) -> str:
    """One case-insensitive pattern string (for Polars' Rust regex engine)
    alternating every boilerplate segment, plus the URL and edition-header
    patterns.
    """
    segment_patterns = [
        _segment_to_flexible_pattern(segment) for segment in boilerplate_segments
    ]
    body = "|".join([*segment_patterns, URL_PATTERN, EDITION_HEADER_PATTERN])
    return f"(?i){body}"


def clean_descriptions(descriptions: pl.Series, boilerplate_pattern: str) -> pl.Series:
    """Strip boilerplate/URL/edition-header matches, then tidy whitespace.

    Texts without a match are returned unchanged (aside from the idempotent
    whitespace tidy-up, which has no effect on already-clean text).
    """
    return (
        descriptions.str.replace_all(boilerplate_pattern, " ")
        .str.replace_all(r"\s*\n\s*", "\n")
        .str.replace_all(r"[ \t]+", " ")
        .str.strip_chars()
    )
