"""Corpus splitting (spec section 4.4).

An item's corpus is its GTL id cut to ``split_level`` levels. The same corpus
id can hold either "this genre and everything below it" (when the item's GTL
depth >= split_level) or "items classified only to this genre" (when the
item's GTL depth < split_level) -- ``corpus_depth`` disambiguates the two.
"""

GTL_ID_LENGTH = 8
PAIR_LENGTH = 2
EMPTY_PAIR = "00"


def corpus_id(gtl_id: str, split_level: int) -> str:
    """The corpus an item belongs to, given its GTL id and the split level.

    Args:
        gtl_id: The item's 8-digit GTL id (2 digits per level, "00" for
            unused levels).
        split_level: 0 (one corpus) to 4 (full GTL depth).

    Returns:
        An 8-digit corpus id. ``"00000000"`` at split level 0.
    """
    return gtl_id[: PAIR_LENGTH * split_level].ljust(GTL_ID_LENGTH, "0")


def corpus_depth(gtl_id: str) -> int:
    """The number of non-"00" pairs from the left of an 8-digit GTL/corpus id.

    E.g. ``"01000000"`` -> 1, ``"01010000"`` -> 2, ``"01010203"`` -> 4.
    """
    depth = 0
    for start in range(0, GTL_ID_LENGTH, PAIR_LENGTH):
        if gtl_id[start : start + PAIR_LENGTH] == EMPTY_PAIR:
            break
        depth += 1
    return depth


def is_small_corpus(n_items: int, min_corpus_items: int) -> bool:
    """Whether a corpus is too small to model (spec section 4.4)."""
    return n_items < min_corpus_items
