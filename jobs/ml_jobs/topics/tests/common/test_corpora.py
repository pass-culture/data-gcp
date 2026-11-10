import pytest

from src.common.corpora import corpus_depth, corpus_id, is_small_corpus

# Spec section 4.4 table: one item at each depth, its corpus at each split level.
SECTION_4_4_TABLE = [
    # (gtl_id, {split_level: expected_corpus_id})
    (
        "01000000",
        {0: "00000000", 1: "01000000", 2: "01000000", 3: "01000000", 4: "01000000"},
    ),
    (
        "01010000",
        {0: "00000000", 1: "01000000", 2: "01010000", 3: "01010000", 4: "01010000"},
    ),
    (
        "01010200",
        {0: "00000000", 1: "01000000", 2: "01010000", 3: "01010200", 4: "01010200"},
    ),
    (
        "01010203",
        {0: "00000000", 1: "01000000", 2: "01010000", 3: "01010200", 4: "01010203"},
    ),
]


@pytest.mark.parametrize(
    ("gtl_id", "split_level", "expected"),
    [
        (gtl_id, split_level, expected)
        for gtl_id, by_level in SECTION_4_4_TABLE
        for split_level, expected in by_level.items()
    ],
)
def test_corpus_id_matches_section_4_4_table(
    gtl_id: str, split_level: int, expected: str
) -> None:
    assert corpus_id(gtl_id, split_level) == expected


def test_level_4_keeps_shallower_items_in_their_own_corpus() -> None:
    """An item classified only to 01010000 is modelled apart from a more
    specific 01010203 item, even though 01010203's level-2/3 ancestor is
    01010000.
    """
    shallow_corpus = corpus_id("01010000", split_level=4)
    specific_corpus = corpus_id("01010203", split_level=4)
    assert shallow_corpus != specific_corpus


@pytest.mark.parametrize(
    ("gtl_id", "expected_depth"),
    [
        ("00000000", 0),
        ("01000000", 1),
        ("01010000", 2),
        ("01010200", 3),
        ("01010203", 4),
    ],
)
def test_corpus_depth(gtl_id: str, expected_depth: int) -> None:
    assert corpus_depth(gtl_id) == expected_depth


@pytest.mark.parametrize(
    ("n_items", "min_corpus_items", "expected"),
    [
        (999, 1000, True),
        (1000, 1000, False),
        (5000, 1000, False),
        (0, 1000, True),
    ],
)
def test_is_small_corpus(
    n_items: int,
    min_corpus_items: int,
    expected: bool,  # noqa: FBT001
) -> None:
    assert is_small_corpus(n_items, min_corpus_items) == expected
