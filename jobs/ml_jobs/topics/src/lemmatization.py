"""
NLP columns for *every* book item, stored in the cached item table
(data/items__<project>__<dataset>.parquet):

    noun_lemmas  list<str>  common nouns (POS == NOUN), lemmatized, lowercased
    verb_lemmas  list<str>  verbs (POS == VERB; auxiliaries excluded), idem
    entities     str        optional (entities=True): named entities as JSON,
                            exactly as `preprocess` builds them for a sample
                            (nlp_preprocessing.extract_entities: label
                            aliases, entity-type allowlist, cleaning)

Descriptions are boilerplate-stripped first (see text_cleaning), so publisher
templates don't add "information", "édition", BnF, ... to every item. Items
in a language without a spaCy pipeline (see SPACY_MODELS) get nulls; an empty
description gets empty lists and "{}".

The parser is always disabled, NER too unless entities are asked for: this is
the whole table (~1.7M distinct French descriptions at ~60 docs/s per core
without NER), so it runs in parallel processes over row batches of the
Parquet file, which is then rewritten with the extra columns -- never fully
loaded in memory.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from src.config import ENTITIES_COL, LEMMA_COLS
from src.nlp_preprocessing import DEFAULT_ENTITY_TYPES, SPACY_MODELS, extract_entities
from src.text_cleaning import boilerplate_regex, find_boilerplate_segments, strip_all

if TYPE_CHECKING:
    from collections.abc import Collection
    from pathlib import Path

TEXT_COL = "offer_description"

LEMMA_TYPE = pa.list_(pa.large_string())

# Texts sent to a worker at once: large enough to amortize inter-process
# transfer, small enough to keep all workers busy until the end of a batch.
_CHUNK_TEXTS = 500

# Per worker process: its spaCy pipeline, and the entity types to keep (None
# = no NER).
_NLP = None
_ENTITY_TYPES: tuple[str, ...] | None = None


def _init_worker(lang: str, entity_types: tuple[str, ...] | None) -> None:
    global _NLP, _ENTITY_TYPES  # one pipeline per worker process
    import spacy

    disable = ["parser"] if entity_types is not None else ["parser", "ner"]
    _NLP = spacy.load(SPACY_MODELS[lang], disable=disable)
    _ENTITY_TYPES = entity_types


def doc_lemmas(doc) -> tuple[list[str], list[str]]:
    """(noun lemmas, verb lemmas) of a spaCy doc, lowercased, in text order.
    Lemmas without any letter (stray "-", digits) are dropped."""
    nouns: list[str] = []
    verbs: list[str] = []
    for tok in doc:
        target = {"NOUN": nouns, "VERB": verbs}.get(tok.pos_)
        if target is None:
            continue
        lemma = tok.lemma_.lower().strip()
        if any(c.isalpha() for c in lemma):
            target.append(lemma)
    return nouns, verbs


def _process_chunk(texts: list[str]) -> list[tuple]:
    """(nouns, verbs, entities JSON or None) per text."""
    out = []
    for doc in _NLP.pipe(texts, batch_size=256):
        nouns, verbs = doc_lemmas(doc)
        entities = (
            None if _ENTITY_TYPES is None else extract_entities(doc, _ENTITY_TYPES)
        )
        out.append((nouns, verbs, entities))
    return out


def process_texts(texts: list[str], pool: ProcessPoolExecutor) -> list[tuple]:
    """_process_chunk for each text, spread over `pool`'s workers
    (initialized with _init_worker). Order is preserved."""
    chunks = [texts[i : i + _CHUNK_TEXTS] for i in range(0, len(texts), _CHUNK_TEXTS)]
    return [res for chunk in pool.map(_process_chunk, chunks) for res in chunk]


def nlp_columns(
    batch: pl.DataFrame, regex: str, pools: dict[str, ProcessPoolExecutor]
) -> tuple[list, list, list]:
    """noun_lemmas / verb_lemmas / entities values for one batch of items.
    Each distinct (stripped) description is processed once: many editions
    share one."""
    n = len(batch)
    nouns: list = [None] * n
    verbs: list = [None] * n
    entities: list = [None] * n
    texts = strip_all(batch[TEXT_COL], regex).fill_null("").to_list()
    langs = batch["lang"].to_list()
    for lang, pool in pools.items():
        rows = [i for i, code in enumerate(langs) if code == lang]
        distinct = list(dict.fromkeys(texts[i] for i in rows))
        by_text = dict(zip(distinct, process_texts(distinct, pool), strict=True))
        for i in rows:
            nouns[i], verbs[i], entities[i] = by_text[texts[i]]
    for i, text in enumerate(texts):  # empty descriptions have no lang
        if not text.strip():
            nouns[i], verbs[i], entities[i] = [], [], "{}"
    return nouns, verbs, entities


def add_lemmas(
    cache: Path,
    *,
    entities: bool = False,
    entity_types: Collection[str] = DEFAULT_ENTITY_TYPES,
    batch_rows: int = 100_000,
    n_jobs: int | None = None,
    force: bool = False,
) -> None:
    """Add noun_lemmas / verb_lemmas (and `entities` if asked) to the item
    table at `cache`, in place (written to a temporary file, then swapped
    in). Skipped if those columns already exist, unless `force`. Other
    columns -- including an `entities` column from a previous run, when not
    recomputed -- are kept."""
    computed = (*LEMMA_COLS, ENTITIES_COL) if entities else LEMMA_COLS
    names = pq.read_schema(cache).names
    if "lang" not in names:
        raise ValueError(f"{cache} has no `lang` column: run load-data first")
    if set(computed) <= set(names) and not force:
        logger.info(f"{cache} already has {computed} (use --force to recompute)")
        return
    columns = [c for c in names if c not in computed]
    langs = list(SPACY_MODELS)
    types = tuple(entity_types) if entities else None

    descriptions = (
        pl.scan_parquet(cache)
        .filter(pl.col("lang").is_in(langs))
        .select(TEXT_COL)
        .collect()[TEXT_COL]
    )
    regex = boilerplate_regex(find_boilerplate_segments(descriptions))
    del descriptions

    source = pq.ParquetFile(cache)
    schema = source.schema_arrow
    schema = pa.schema([schema.field(c) for c in columns]).with_metadata(None)
    for col in LEMMA_COLS:
        schema = schema.append(pa.field(col, LEMMA_TYPE))
    if entities:
        schema = schema.append(pa.field(ENTITIES_COL, pa.large_string()))
    total = source.metadata.num_rows
    tmp = cache.with_name(cache.name + ".tmp")
    n_jobs = n_jobs or os.cpu_count() or 1
    what = "nouns + verbs" + (f" + entities ({', '.join(types)})" if entities else "")
    logger.info(
        f"Extracting {what} of {total} items ({', '.join(langs)}) "
        f"on {n_jobs} processes..."
    )

    pools = {
        lang: ProcessPoolExecutor(
            n_jobs, initializer=_init_worker, initargs=(lang, types)
        )
        for lang in langs
    }
    start, done = time.time(), 0
    try:
        with pq.ParquetWriter(tmp, schema) as writer:
            for record_batch in source.iter_batches(batch_rows, columns=columns):
                table = pa.Table.from_batches([record_batch])
                nouns, verbs, ents = nlp_columns(pl.from_arrow(table), regex, pools)
                for col, values in zip(LEMMA_COLS, (nouns, verbs), strict=True):
                    table = table.append_column(col, pa.array(values, LEMMA_TYPE))
                if entities:
                    table = table.append_column(
                        ENTITIES_COL, pa.array(ents, pa.large_string())
                    )
                writer.write_table(table.cast(schema))
                done += len(table)
                rate = done / (time.time() - start)
                logger.info(
                    f"{done}/{total} items ({rate:.0f}/s, "
                    f"~{(total - done) / rate / 60:.0f} min left)"
                )
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    finally:
        for pool in pools.values():
            pool.shutdown(cancel_futures=True)
    os.replace(tmp, cache)
    logger.info(f"Wrote {computed} to {cache}")
