"""
Sequential multilingual NLP preprocessing of book offer descriptions: detect
each description's language, then run language-specific entity extraction
(NER) and noun lemmatization through spaCy.

Pipeline (per description)
---------------------------
1. detect_languages() : (src/language.py) guesses an ISO 639-1 code per
                         description, in parallel for large inputs. Skipped
                         when the input already has a `lang` column (computed
                         once on the full item table, see data_loading).
                         Short descriptions are inherently harder to
                         classify correctly -- a general limitation of
                         language ID, not specific to this module.
2. nlp_for_lang()      : lazily loads/caches the spaCy pipeline for that
                         language (fr/en/de/es by default -- see
                         SPACY_MODELS). A description in an unsupported
                         language is left unprocessed and flagged
                         ("unsupported_lang") rather than forced through
                         the wrong pipeline.
3. extract_entities()  : named entities grouped by type, labels unified
                         across languages and filtered to an allowlist
                         (ENTITY_LABEL_ALIASES, DEFAULT_ENTITY_TYPES): each
                         spaCy pipeline was trained on its own NER scheme.
4. extract_nouns()     : common nouns only (POS == NOUN, and not already
                         part of a named entity), lemmatized.
"""

from __future__ import annotations

import json
from collections import defaultdict
from typing import TYPE_CHECKING

import pandas as pd
from loguru import logger

from src.language import detect_languages

if TYPE_CHECKING:
    from collections.abc import Collection

# en_core_web_sm (not en_core_news_sm, unlike the other 3) is spaCy's
# standard small English pipeline.
SPACY_MODELS = {
    "fr": "fr_core_news_sm",
    # "en": "en_core_web_sm",
    # "de": "de_core_news_sm",
    # "es": "es_core_news_sm",
}

_NLP_CACHE: dict[str, object] = {}


def nlp_for_lang(lang: str):
    """Lazily load and cache the spaCy pipeline for `lang`, or None if
    unsupported (see SPACY_MODELS)."""
    model_name = SPACY_MODELS.get(lang)
    if model_name is None:
        return None
    if lang not in _NLP_CACHE:
        import spacy

        # Models are pinned as wheels in pyproject.toml -- run `uv sync`
        # rather than downloading at runtime.
        _NLP_CACHE[lang] = spacy.load(model_name, disable=["parser"])
    return _NLP_CACHE[lang]


# One label scheme across languages: the fr/de/es pipelines use WikiNER's
# PER/LOC/ORG/MISC, en_core_web_sm uses OntoNotes' 18 types. Labels are
# renamed with ENTITY_LABEL_ALIASES (persons -> PERSON; locations, products,
# works, facilities, nationalities -> EXTRA), others keep their own name.
# WikiNER MISC has no alias, so it is dropped unless asked for.
#
# Glossary
# --------
# WikiNER (fr/de/es_core_news_sm) -- Nothman et al. 2013, "Learning
# multilingual named entity recognition from Wikipedia", Artificial
# Intelligence 194, https://doi.org/10.1016/j.artint.2012.03.006 -- follows
# the CoNLL-2003 scheme (Tjong Kim Sang & De Meulder 2003, §1-2,
# https://aclanthology.org/W03-0419.pdf; "Mostly, MUC conventions were
# followed"):
#   PER   persons
#   LOC   locations -- *including* countries and cities: CoNLL has no GPE
#         (fr_core_news_sm tags France, Paris, Londres as LOC)
#   ORG   organizations
#   MISC  "names of miscellaneous entities that do not belong to the previous
#         three groups" (spaCy: events, nationalities, products, works of art)
# Caveat: spacy.explain("LOC") returns the OntoNotes definition ("Non-GPE
# locations") for every model, since spaCy's glossary has a single LOC key;
# it is wrong for the WikiNER models.
#
# OntoNotes 5.0 (en_core_web_sm) -- OntoNotes Release 5.0, LDC2013T19, §2.6
# "Entity Names Annotation", pp. 21-22,
# https://catalog.ldc.upenn.edu/docs/LDC2013T19/OntoNotes-Release-5.0.pdf
# Names:
#   PERSON       People, including fictional
#   NORP         Nationalities or religious or political groups
#   FAC          Buildings, airports, highways, bridges, etc.
#   ORG          Companies, agencies, institutions, etc.
#   GPE          Countries, cities, states
#   LOC          Non-GPE locations, mountain ranges, bodies of water
#   PRODUCT      Vehicles, weapons, foods, etc. (Not services)
#   EVENT        Named hurricanes, battles, wars, sports events, etc.
#   WORK_OF_ART  Titles of books, songs, etc.
#   LAW          Named documents made into laws
#   LANGUAGE     Any named language
# Values "annotated in a style similar to names":
#   DATE         Absolute or relative dates or periods
#   TIME         Times smaller than a day
#   PERCENT      Percentage (including "%")
#   MONEY        Monetary values, including unit
#   QUANTITY     Measurements, as of weight or distance
#   ORDINAL      "first", "second"
#   CARDINAL     Numerals that do not fall under another type
ENTITY_LABEL_ALIASES = {
    "PER": "PERSON",  # WikiNER (fr) persons, named like OntoNotes (en)
    "LOC": "EXTRA",
    "PRODUCT": "EXTRA",
    "WORK_OF_ART": "EXTRA",
    "FAC": "EXTRA",
    "NORP": "EXTRA",
}

# Entity types kept by default (after aliasing). The French pipeline only
# yields PERSON, ORG and EXTRA here; GPE, DATE and EVENT come from the English
# one. Pick others with --entity-types (e.g. PERSON,ORG,MISC).
DEFAULT_ENTITY_TYPES = ("PERSON", "GPE", "ORG", "DATE", "EVENT", "EXTRA")


# Longer "entities" are almost always run-on spans from the small model
# (e.g. "Tremblement de terre l'empêche de rentrer chez").
MAX_ENTITY_TOKENS = 6


def clean_entity(ent) -> str | None:
    """Fix or reject the small models' most common NER mistakes:
    - leading lowercase tokens: "la France" -> "France", "ligne Firecrackers"
      -> "Firecrackers" (also merges duplicates like France / la France);
    - URLs and emails tagged LOC/ORG ("www.hachettebnf.fr");
    - single verbs / adjectives ("Découvrez", "Française");
    - run-on spans longer than MAX_ENTITY_TOKENS.
    Returns the cleaned entity text, or None to drop it."""
    tokens = list(ent)
    while tokens and not (tokens[0].text[0].isupper() or tokens[0].text[0].isdigit()):
        tokens.pop(0)
    if not tokens or len(tokens) > MAX_ENTITY_TOKENS:
        return None
    if any(t.like_url or t.like_email for t in tokens):
        return None
    if len(tokens) == 1 and tokens[0].pos_ in {"VERB", "AUX", "ADJ"}:
        return None
    return ent.doc[tokens[0].i : tokens[-1].i + 1].text.strip() or None


def extract_entities(doc, entity_types: Collection[str] = DEFAULT_ENTITY_TYPES) -> str:
    """Named entities grouped by type, as JSON (entity texts may contain any
    separator), e.g. '{"LOC": ["Paris"], "PER": ["Victor Hugo"]}'. Labels are
    unified across languages (ENTITY_LABEL_ALIASES) and only `entity_types`
    are kept; see clean_entity for the per-entity filtering."""
    by_type: dict[str, list[str]] = defaultdict(list)
    for ent in doc.ents:
        label = ENTITY_LABEL_ALIASES.get(ent.label_, ent.label_)
        if label not in entity_types:
            continue
        text = clean_entity(ent)
        if text and text not in by_type[label]:
            by_type[label].append(text)
    return json.dumps(dict(sorted(by_type.items())), ensure_ascii=False)


def extract_nouns(doc) -> str:
    """Common nouns only (not part of a named entity), lemmatized and
    lowercased."""
    return " ".join(
        tok.lemma_.lower() for tok in doc if tok.pos_ == "NOUN" and not tok.ent_type_
    )


def enrich(
    docs: list[str],
    langs: list[str],
    entity_types: Collection[str] = DEFAULT_ENTITY_TYPES,
) -> pd.DataFrame:
    """Run each description through its language's spaCy pipeline, batched
    per language (nlp.pipe) rather than one description at a time, then
    extract entities + nouns. Descriptions in an unsupported language are
    flagged, not dropped."""
    n = len(docs)
    status = ["ok"] * n
    entities = ["{}"] * n
    nouns = [""] * n

    by_lang: dict[str, list[int]] = defaultdict(list)
    for i, lang in enumerate(langs):
        by_lang[lang].append(i)

    for lang, idx in by_lang.items():
        nlp = nlp_for_lang(lang)
        if nlp is None:
            logger.warning(
                f"{len(idx)} descriptions in unsupported language '{lang}' "
                f"-- flagged, not processed"
            )
            for i in idx:
                status[i] = "unsupported_lang"
            continue
        subset = [docs[i] for i in idx]
        for i, doc in zip(idx, nlp.pipe(subset, batch_size=256), strict=False):
            entities[i] = extract_entities(doc, entity_types)
            nouns[i] = extract_nouns(doc)

    return pd.DataFrame(
        {"lang": langs, "nlp_status": status, "entities": entities, "nouns": nouns}
    )


def preprocess(
    sample: pd.DataFrame,
    text_col: str = "offer_description",
    entity_types: Collection[str] = DEFAULT_ENTITY_TYPES,
) -> pd.DataFrame:
    """End-to-end: detect language (unless the sample already carries a
    `lang` column) then enrich, attached to the input sample."""
    docs = sample[text_col].tolist()
    if "lang" in sample.columns:
        logger.info("Reusing the sample's precomputed `lang` column")
        langs = sample["lang"].tolist()
        sample = sample.drop(columns="lang")
    else:
        logger.info("Detecting language per description...")
        langs = detect_languages(docs)
    logger.info("\n" + pd.Series(langs).value_counts().to_string())

    logger.info(
        f"Running NER + noun lemmatization (supported: {', '.join(SPACY_MODELS)}; "
        f"entity types kept: {', '.join(entity_types)})..."
    )
    enrichment = enrich(docs, langs, entity_types)
    logger.info("\n" + enrichment["nlp_status"].value_counts().to_string())

    return pd.concat([sample.reset_index(drop=True), enrichment], axis=1)
