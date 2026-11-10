import datetime

import polars as pl

from src.vocabulary.registry import (
    build_stop_word_entries,
    compute_term_frequencies,
    merge_vocabulary,
    update_vocabulary,
)

NOW = datetime.datetime(2026, 11, 10, tzinfo=datetime.UTC)


def _documents(
    item_ids: list[str],
    gtl_ids: list[str],
    noun_lemmas: list[list[str]],
    verb_lemmas: list[list[str]],
) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "item_id": item_ids,
            "gtl_id": gtl_ids,
            "noun_lemmas": noun_lemmas,
            "verb_lemmas": verb_lemmas,
        }
    )


def test_compute_term_frequencies_counts_distinct_items() -> None:
    documents = _documents(
        ["1", "2"],
        ["01000000", "01000000"],
        [["roman", "roman"], ["roman"]],  # "roman" repeated within item 1
        [[], []],
    )
    frequencies = compute_term_frequencies(documents)
    roman = frequencies.filter(pl.col("term") == "roman").row(0, named=True)
    assert roman["df_total"] == 2
    assert roman["pos"] == "NOUN"


def test_compute_term_frequencies_marks_both_when_seen_as_noun_and_verb() -> None:
    documents = _documents(
        ["1", "2"],
        ["01000000", "01000000"],
        [["roman"], []],
        [[], ["roman"]],
    )
    frequencies = compute_term_frequencies(documents)
    roman = frequencies.filter(pl.col("term") == "roman").row(0, named=True)
    assert roman["pos"] == "BOTH"


def test_compute_term_frequencies_breaks_down_df_by_gtl() -> None:
    documents = _documents(
        ["1", "2", "3"],
        ["01000000", "01000000", "02000000"],
        [["roman"], ["roman"], ["roman"]],
        [[], [], []],
    )
    frequencies = compute_term_frequencies(documents)
    roman = frequencies.filter(pl.col("term") == "roman").row(0, named=True)
    by_gtl = {entry["gtl_id"]: entry["df"] for entry in roman["df_by_gtl"]}
    assert by_gtl == {"01000000": 2, "02000000": 1}


def test_compute_term_frequencies_empty_documents_returns_empty_frame() -> None:
    documents = _documents([], [], [], [])
    frequencies = compute_term_frequencies(documents)
    assert frequencies.height == 0
    assert "term" in frequencies.columns


def test_build_stop_word_entries_excludes_known_terms() -> None:
    entries = build_stop_word_entries({"le", "la"}, known_terms={"le"})
    assert entries["term"].to_list() == ["la"]
    assert entries["is_stop_word"].to_list() == [True]
    assert entries["df_total"].to_list() == [0]


def test_merge_vocabulary_assigns_sequential_ids_on_first_run() -> None:
    term_rows = pl.DataFrame(
        {
            "term": ["roman", "aimer"],
            "pos": ["NOUN", "VERB"],
            "df_total": [3, 1],
            "df_by_gtl": [[], []],
            "is_stop_word": [False, False],
        }
    )
    vocabulary = merge_vocabulary(term_rows, None, now=NOW)
    assert set(vocabulary["term_id"].to_list()) == {1, 2}
    assert vocabulary["first_seen"].to_list() == [NOW, NOW]
    assert vocabulary["last_seen"].to_list() == [NOW, NOW]


def test_merge_vocabulary_keeps_existing_term_id_and_first_seen() -> None:
    existing = pl.DataFrame(
        {
            "term_id": [5],
            "term": ["roman"],
            "first_seen": [datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)],
        }
    )
    term_rows = pl.DataFrame(
        {
            "term": ["roman", "nouveau"],
            "pos": ["NOUN", "NOUN"],
            "df_total": [3, 1],
            "df_by_gtl": [[], []],
            "is_stop_word": [False, False],
        }
    )
    vocabulary = merge_vocabulary(term_rows, existing, now=NOW)

    roman = vocabulary.filter(pl.col("term") == "roman").row(0, named=True)
    assert roman["term_id"] == 5
    assert roman["first_seen"] == datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    assert roman["last_seen"] == NOW

    nouveau = vocabulary.filter(pl.col("term") == "nouveau").row(0, named=True)
    assert nouveau["term_id"] == 6
    assert nouveau["first_seen"] == NOW


def test_update_vocabulary_registers_stop_words_alongside_real_terms() -> None:
    documents = _documents(["1"], ["01000000"], [["roman"]], [[]])
    vocabulary = update_vocabulary(
        documents, stop_words={"chose"}, existing_vocabulary=None, now=NOW
    )

    roman = vocabulary.filter(pl.col("term") == "roman").row(0, named=True)
    assert roman["is_stop_word"] is False
    assert roman["df_total"] == 1

    chose = vocabulary.filter(pl.col("term") == "chose").row(0, named=True)
    assert chose["is_stop_word"] is True
    assert chose["df_total"] == 0
