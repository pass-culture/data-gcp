import numpy as np
import pandas as pd

from src.embeddings import (
    EMBEDDING_COL,
    embeddings_cache_path,
    load_embeddings,
    to_matrix,
)


def test_to_matrix_renormalizes_truncated_vectors():
    m = to_matrix(pd.Series([[3.0, 4.0], [0.0, 0.0], [1.0, 0.0]]))
    np.testing.assert_allclose(np.linalg.norm(m, axis=1), [1, 0, 1])
    np.testing.assert_allclose(m[0], [0.6, 0.8])


def test_load_embeddings_caches_found_and_absent_items(tmp_path, monkeypatch):
    queried: list[list[str]] = []
    table = {"a": [1.0, 0.0], "b": [0.0, 1.0]}  # "c" has no embedding

    def fake_query(item_ids, project, dataset):
        queried.append(item_ids)
        ids = [i for i in item_ids if i in table]
        return pd.DataFrame({"item_id": ids, EMBEDDING_COL: [table[i] for i in ids]})

    monkeypatch.setattr("src.embeddings._query_embeddings", fake_query)

    got = load_embeddings(["a", "c"], str(tmp_path), "p", "d")
    assert got["item_id"].tolist() == ["a"]
    assert queried == [["a", "c"]]

    # "a" and "c" are both cached now: only "b" is queried
    got = load_embeddings(["a", "b", "c"], str(tmp_path), "p", "d")
    assert sorted(got["item_id"]) == ["a", "b"]
    assert queried[-1] == ["b"]

    # everything known: no query at all, i.e. no new full-table scan
    load_embeddings(["a", "b", "c"], str(tmp_path), "p", "d")
    assert len(queried) == 2
    assert embeddings_cache_path(str(tmp_path), "p", "d").exists()
