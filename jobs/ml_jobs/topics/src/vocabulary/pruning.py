"""Step 4 of the vocabulary pipeline: pruning and repair (spec section 7.1),
applied to each lemma in this order: drop initials, repair a broken "oe"
ligature, then drop stop words (checked against the repaired form).
"""

import re

from spacy.lang.fr.stop_words import STOP_WORDS as SPACY_FR_STOP_WORDS

INITIAL_PATTERN = re.compile(r"^\w\.$")
# Matches a lemma that lost or mis-encoded its "oe" ligature, e.g. "uvre",
# "l'uvre", "d'¿uvre", "½uvre", "1/2uvre", "uvrer" -- never "manoeuvre", which
# already has its ligature and so doesn't match.
BROKEN_OE_PATTERN = re.compile(r"^(?:[ld]['’]?)?(?:\?|¿+|½|1/2)?uvre(r)?$")  # noqa: RUF001


def is_initial(lemma: str) -> bool:
    return bool(INITIAL_PATTERN.match(lemma))


def repair_lemma(lemma: str) -> str:
    """Fix a broken "oe" ligature; lemmas that don't match are unchanged."""
    match = BROKEN_OE_PATTERN.match(lemma)
    if not match:
        return lemma
    return "oeuvrer" if match.group(1) else "oeuvre"


def build_stop_words(
    generic_stop_words: list[str], forbidden_words: list[str]
) -> set[str]:
    """The union of spaCy's French stop words, the generic list, and the
    catalogue/abbreviation-specific forbidden list.
    """
    return SPACY_FR_STOP_WORDS | set(generic_stop_words) | set(forbidden_words)


def prune_lemmas(lemmas: list[str], stop_words: set[str]) -> list[str]:
    """Drop initials and stop words, repairing a broken "oe" ligature in what's
    kept.
    """
    kept = []
    for lemma in lemmas:
        if is_initial(lemma):
            continue
        repaired = repair_lemma(lemma)
        if repaired in stop_words:
            continue
        kept.append(repaired)
    return kept


def build_document(noun_lemmas: list[str], verb_lemmas: list[str]) -> str:
    """The kept lemmas, nouns then verbs, joined with spaces."""
    return " ".join([*noun_lemmas, *verb_lemmas])
