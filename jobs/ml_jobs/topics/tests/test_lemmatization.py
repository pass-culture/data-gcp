import json

import pandas as pd
import pyarrow.parquet as pq
import pytest

from src.config import ENTITIES_COL, FULL_TABLE_NLP_COLS, LEMMA_COLS
from src.data_loading import load_items
from src.lemmatization import add_lemmas


def _write_items(path, descriptions, langs):
    n = len(descriptions)
    pd.DataFrame(
        {
            "item_id": [str(i) for i in range(n)],
            "offer_name": ["titre"] * n,
            "offer_description": descriptions,
            "gtl_id": ["01000000"] * n,
            "gtl_label_level_1": ["Littérature"] * n,
            "lang": langs,
        }
    ).to_parquet(path, index=False)


@pytest.fixture
def cache(tmp_path):
    path = tmp_path / "items__p__d.parquet"
    _write_items(
        path,
        [
            "Le détective découvre un cadavre dans la bibliothèque.",
            "Le détective découvre un cadavre dans la bibliothèque.",  # shared text
            "The children were eating apples.",
            "",
        ],
        ["fr", "fr", "en", None],
    )
    return path


def test_add_lemmas_writes_lowercased_noun_and_verb_lists(cache):
    add_lemmas(cache, batch_rows=3, n_jobs=1)
    df = pd.read_parquet(cache)
    assert len(df) == 4
    assert list(df.columns[-2:]) == list(LEMMA_COLS)
    nouns, verbs = df.loc[0, "noun_lemmas"], df.loc[0, "verb_lemmas"]
    # Lowercased lemmas ("Bibliothèques" -> "bibliothèque"); the small model
    # isn't perfect, so only check what it reliably tags.
    assert {"cadavre", "bibliothèque"} <= set(nouns)
    assert list(verbs) == ["découvrir"]
    assert list(df.loc[1, "noun_lemmas"]) == list(nouns)
    assert df.loc[2, "noun_lemmas"] is None  # no English pipeline
    assert list(df.loc[3, "noun_lemmas"]) == []
    assert list(df.loc[3, "verb_lemmas"]) == []


def test_add_lemmas_skips_unless_forced(cache):
    add_lemmas(cache, n_jobs=1)
    mtime = cache.stat().st_mtime_ns
    add_lemmas(cache, n_jobs=1)
    assert cache.stat().st_mtime_ns == mtime
    add_lemmas(cache, n_jobs=1, force=True)
    assert pq.read_schema(cache).names.count("noun_lemmas") == 1


def test_entities_are_extracted_like_preprocess(tmp_path):
    path = tmp_path / "items__p__d.parquet"
    texts = ["Victor Hugo a vécu à Paris.", "", "Dickens."]
    _write_items(path, texts, ["fr", None, "en"])
    add_lemmas(path, entities=True, n_jobs=1)
    df = pd.read_parquet(path)
    assert json.loads(df.loc[0, ENTITIES_COL]) == {
        "EXTRA": ["Paris"],
        "PERSON": ["Victor Hugo"],
    }
    assert df.loc[1, ENTITIES_COL] == "{}"  # empty description
    assert pd.isna(df.loc[2, ENTITIES_COL])  # no English pipeline
    only_person = tmp_path / "items__p__e.parquet"
    _write_items(only_person, ["Victor Hugo a vécu à Paris."], ["fr"])
    add_lemmas(only_person, entities=True, entity_types=["PERSON"], n_jobs=1)
    assert pd.read_parquet(only_person).loc[0, ENTITIES_COL] == (
        '{"PERSON": ["Victor Hugo"]}'
    )


def test_entities_from_a_previous_run_are_kept_when_only_lemmatizing(cache):
    add_lemmas(cache, entities=True, n_jobs=1)
    before = pd.read_parquet(cache)[ENTITIES_COL]
    add_lemmas(cache, n_jobs=1, force=True)
    after = pd.read_parquet(cache)
    pd.testing.assert_series_equal(after[ENTITIES_COL], before)
    assert after.columns.tolist().count(ENTITIES_COL) == 1


def test_load_items_leaves_full_table_nlp_columns_out(cache):
    add_lemmas(cache, entities=True, n_jobs=1)
    df = load_items(str(cache.parent), "p", "d", lang="fr")
    assert not set(FULL_TABLE_NLP_COLS) & set(df.columns)
    assert len(df) == 2
