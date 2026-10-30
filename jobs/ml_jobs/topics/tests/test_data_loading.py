import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from src.data_loading import (
    add_languages,
    cache_path,
    load_and_sample,
    load_items,
    recursive_stratified_sample,
    waterfill_quotas,
)

PROJECT_ROOT = Path(__file__).parent.parent


@pytest.mark.parametrize(
    ("sizes", "n_total", "expected"),
    [
        ({"a": 10, "b": 10, "c": 10}, 9, {"a": 3, "b": 3, "c": 3}),
        # small groups are capped, their slack goes to the others
        ({"a": 1, "b": 100, "c": 100}, 21, {"a": 1, "b": 10, "c": 10}),
        # budget larger than the pool -> everything
        ({"a": 2, "b": 3}, 100, {"a": 2, "b": 3}),
        # integer leftover goes to the first groups in sorted order
        ({"c": 10, "b": 10, "a": 10}, 2, {"a": 1, "b": 1, "c": 0}),
        ({"a": 5}, 0, {"a": 0}),
    ],
)
def test_waterfill_quotas(sizes, n_total, expected):
    assert waterfill_quotas(sizes, n_total) == expected


def test_waterfill_quotas_never_exceeds_sizes():
    rng = np.random.default_rng(0)
    for _ in range(200):
        sizes = {f"g{i}": int(rng.integers(1, 50)) for i in range(rng.integers(1, 10))}
        n_total = int(rng.integers(0, 300))
        quota = waterfill_quotas(sizes, n_total)
        assert sum(quota.values()) == min(n_total, sum(sizes.values()))
        assert all(0 <= quota[g] <= sizes[g] for g in sizes)


def _items(n: int = 2000) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    l1 = rng.choice(["A", "B", "C"], n, p=[0.8, 0.15, 0.05])
    l2 = [f"{a}{rng.integers(0, 3)}" if rng.random() > 0.2 else None for a in l1]
    return pd.DataFrame(
        {"item_id": range(n), "gtl_label_level_1": l1, "gtl_label_level_2": l2}
    )


def test_recursive_stratified_sample_balances_level_1():
    sample = recursive_stratified_sample(
        _items(), ["gtl_label_level_1", "gtl_label_level_2"], 150, seed=42
    )
    assert len(sample) == 150
    assert sample["item_id"].is_unique
    counts = sample["gtl_label_level_1"].value_counts()
    assert counts.max() - counts.min() <= 1


def test_items_without_sublabel_are_kept():
    sample = recursive_stratified_sample(
        _items(), ["gtl_label_level_1", "gtl_label_level_2"], 300, seed=42
    )
    assert sample["gtl_label_level_2"].isna().any()


def test_sample_is_reproducible_across_hash_seeds():
    """Same seed must give the same sample regardless of PYTHONHASHSEED."""
    script = (
        "from loguru import logger; logger.remove()\n"
        "from tests.test_data_loading import _items\n"
        "from src.data_loading import recursive_stratified_sample\n"
        "s = recursive_stratified_sample(_items(), "
        "['gtl_label_level_1', 'gtl_label_level_2'], 101, seed=42)\n"
        "print(s['item_id'].tolist())\n"
    )
    outputs = {
        subprocess.run(
            [sys.executable, "-c", script],
            cwd=PROJECT_ROOT,
            env={**os.environ, "PYTHONHASHSEED": str(h)},
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for h in range(4)
    }
    assert len(outputs) == 1


def test_empty_sample_does_not_crash():
    sample = recursive_stratified_sample(_items(), ["gtl_label_level_1"], 0, seed=42)
    assert sample.empty
    assert list(sample.columns) == list(_items().columns)


def test_cache_is_keyed_by_table_and_refreshable(tmp_path, monkeypatch):
    queried = []

    class FakeClient:
        def __init__(self, project):
            self.project = project

        def query(self, sql):
            queried.append(sql)
            return self

        def to_dataframe(self):
            return pd.DataFrame(
                {
                    "item_id": [self.project],
                    "gtl_id": ["1"],
                    "offer_description": ["Un roman sur la mer et les marins."],
                }
            )

    monkeypatch.setattr("google.cloud.bigquery.Client", FakeClient)

    prod = load_items(str(tmp_path), "proj-prod", "ml_input_prod")
    assert load_items(str(tmp_path), "proj-prod", "ml_input_prod").equals(prod)
    assert len(queried) == 1  # second call served from cache

    ehp = load_items(str(tmp_path), "proj-ehp", "ml_input_dev")
    assert ehp["item_id"].tolist() == ["proj-ehp"]  # not prod's cache
    assert len(queried) == 2

    load_items(str(tmp_path), "proj-prod", "ml_input_prod", refresh=True)
    assert len(queried) == 3
    assert cache_path(str(tmp_path), "proj-prod", "ml_input_prod").exists()


def test_cache_without_lang_is_backfilled(tmp_path):
    path = cache_path(str(tmp_path), "p", "d")
    pd.DataFrame(
        {"item_id": ["1"], "offer_description": ["Un roman sur la mer."]}
    ).to_parquet(path)
    assert load_items(str(tmp_path), "p", "d")["lang"].tolist() == ["fr"]
    assert "lang" in pd.read_parquet(path).columns  # persisted, not recomputed


def test_add_languages():
    df = pd.DataFrame(
        {
            "offer_description": [
                "Un roman sur la mer et les marins bretons.",
                "A novel about the sea and sailors.",
                "Un roman sur la mer et les marins bretons.",
                "  ",
                None,
            ]
        }
    )
    assert add_languages(df)["lang"].tolist() == ["fr", "en", "fr", pd.NA, pd.NA]


def test_load_and_sample_keeps_only_requested_language(tmp_path, monkeypatch):
    items = _items(300)
    items["offer_description"] = [
        f"Une description de roman assez longue pour être gardée, numéro {i}."
        for i in range(300)
    ]
    items["lang"] = ["fr", "fr", "en"] * 100
    monkeypatch.setattr("src.data_loading.load_items", lambda *a, **k: items)
    fr = load_and_sample(None, "p", 1_000, 1, seed=0, lang="fr")
    assert len(fr) == 200
    assert set(fr["lang"]) == {"fr"}
    assert len(load_and_sample(None, "p", 1_000, 1, seed=0, lang=None)) == 300


def test_load_items_reads_only_requested_language(tmp_path):
    path = cache_path(str(tmp_path), "p", "d")
    pd.DataFrame(
        {
            "item_id": ["1", "2", "3"],
            "offer_description": ["a", "b", "c"],
            "lang": ["fr", "en", "fr"],
        }
    ).to_parquet(path)
    assert load_items(str(tmp_path), "p", "d", lang="fr")["item_id"].tolist() == [
        "1",
        "3",
    ]
    assert len(load_items(str(tmp_path), "p", "d")) == 3
