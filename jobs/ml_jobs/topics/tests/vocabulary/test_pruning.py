import pytest

from src.vocabulary.pruning import (
    build_document,
    build_stop_words,
    is_initial,
    prune_lemmas,
    repair_lemma,
)


@pytest.mark.parametrize("lemma", ["a.", "p.", "é.", "t."])
def test_is_initial_true(lemma: str) -> None:
    assert is_initial(lemma)


@pytest.mark.parametrize("lemma", ["roman", "a", "ab.", "mme"])
def test_is_initial_false(lemma: str) -> None:
    assert not is_initial(lemma)


@pytest.mark.parametrize(
    ("broken", "expected"),
    [
        ("uvre", "oeuvre"),
        ("l'uvre", "oeuvre"),
        ("d'¿uvre", "oeuvre"),
        ("½uvre", "oeuvre"),
        ("1/2uvre", "oeuvre"),
        ("uvrer", "oeuvrer"),
    ],
)
def test_repair_lemma_fixes_broken_oe_ligature(broken: str, expected: str) -> None:
    assert repair_lemma(broken) == expected


def test_repair_lemma_leaves_manoeuvre_unchanged() -> None:
    assert repair_lemma("manoeuvre") == "manoeuvre"


def test_repair_lemma_leaves_unrelated_lemma_unchanged() -> None:
    assert repair_lemma("roman") == "roman"


def test_build_stop_words_includes_spacy_generic_and_forbidden() -> None:
    stop_words = build_stop_words(["chose"], ["bnf"])
    assert "le" in stop_words  # spaCy's own French stop word
    assert "chose" in stop_words
    assert "bnf" in stop_words


def test_prune_lemmas_drops_initials() -> None:
    assert prune_lemmas(["t.", "roman"], stop_words=set()) == ["roman"]


def test_prune_lemmas_drops_stop_words() -> None:
    assert prune_lemmas(["roman", "le"], stop_words={"le"}) == ["roman"]


def test_prune_lemmas_checks_stop_words_against_repaired_form() -> None:
    """ "uvre" repairs to "oeuvre" before the stop-word check, so a stop list
    containing "uvre" itself (not "oeuvre") has no effect.
    """
    assert prune_lemmas(["uvre"], stop_words={"uvre"}) == ["oeuvre"]


def test_prune_lemmas_keeps_clean_lemmas() -> None:
    assert prune_lemmas(["roman", "aimer"], stop_words=set()) == ["roman", "aimer"]


def test_build_document_joins_nouns_then_verbs() -> None:
    assert build_document(["roman", "histoire"], ["aimer"]) == "roman histoire aimer"


def test_build_document_empty_lists_gives_empty_string() -> None:
    assert build_document([], []) == ""
