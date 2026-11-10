"""Step 3 of the vocabulary pipeline: spaCy lemmatization (spec section 7.1).

Runs ``nlp.pipe`` in a process pool, ``chunk_texts`` texts per task, with the
parser (and NER, unless enabled) disabled -- lemmatization only needs the
tagger. Each worker loads its own pipeline, since a loaded spaCy model isn't
picklable across process boundaries.
"""

from concurrent.futures import ProcessPoolExecutor
from functools import partial

import spacy


def _load_pipeline(spacy_model: str, *, enable_ner: bool) -> spacy.language.Language:
    disable = ["parser"]
    if not enable_ner:
        disable.append("ner")
    return spacy.load(spacy_model, disable=disable)


def _lemmas_by_pos(doc: spacy.tokens.Doc, pos: set[str]) -> tuple[list[str], list[str]]:
    nouns: list[str] = []
    verbs: list[str] = []
    for token in doc:
        if token.pos_ not in pos:
            continue
        lemma = token.lemma_.strip().lower()
        if not any(char.isalpha() for char in lemma):
            continue
        if token.pos_ == "NOUN":
            nouns.append(lemma)
        elif token.pos_ == "VERB":
            verbs.append(lemma)
    return nouns, verbs


def _lemmatize_chunk(
    texts: list[str], *, spacy_model: str, pos: set[str], enable_ner: bool
) -> list[tuple[list[str], list[str]]]:
    nlp = _load_pipeline(spacy_model, enable_ner=enable_ner)
    return [_lemmas_by_pos(doc, pos) for doc in nlp.pipe(texts)]


def lemmatize_documents(
    texts: list[str],
    *,
    spacy_model: str,
    pos: set[str],
    enable_ner: bool = False,
    chunk_texts: int = 500,
) -> list[tuple[list[str], list[str]]]:
    """One ``(noun_lemmas, verb_lemmas)`` pair per text, in text order."""
    if not texts:
        return []

    chunks = [texts[i : i + chunk_texts] for i in range(0, len(texts), chunk_texts)]
    worker = partial(
        _lemmatize_chunk, spacy_model=spacy_model, pos=pos, enable_ner=enable_ner
    )

    if len(chunks) == 1:
        return worker(chunks[0])

    with ProcessPoolExecutor() as executor:
        chunk_results = executor.map(worker, chunks)
    return [pair for chunk_result in chunk_results for pair in chunk_result]
