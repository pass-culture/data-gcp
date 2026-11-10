import numpy as np
import polars as pl
import pytest

from src.reduction import embeddings as embeddings_module
from src.reduction.embeddings import (
    fetch_embeddings,
    join_with_documents,
    validate_embeddings,
)


def test_fetch_embeddings_l2_normalizes(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = pl.DataFrame({"item_id": ["1", "2"], "embedding": [[3.0, 4.0], [1.0, 0.0]]})
    monkeypatch.setattr(embeddings_module, "read_dataframe", lambda *a, **k: raw)

    result = fetch_embeddings(dim=2, gtl_ids=["01000000"])

    vectors = np.array(result["embedding"].to_list())
    norms = np.linalg.norm(vectors, axis=1)
    assert np.allclose(norms, 1.0)
    assert result["zero_norm"].to_list() == [False, False]


def test_fetch_embeddings_flags_zero_norm_without_dividing_by_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = pl.DataFrame({"item_id": ["1"], "embedding": [[0.0, 0.0]]})
    monkeypatch.setattr(embeddings_module, "read_dataframe", lambda *a, **k: raw)

    result = fetch_embeddings(dim=2, gtl_ids=["01000000"])

    assert result["zero_norm"].to_list() == [True]
    assert result["embedding"].to_list() == [[0.0, 0.0]]


def test_fetch_embeddings_returns_float32_dtype(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = pl.DataFrame({"item_id": ["1"], "embedding": [[1.0, 2.0]]})
    monkeypatch.setattr(embeddings_module, "read_dataframe", lambda *a, **k: raw)

    result = fetch_embeddings(dim=2, gtl_ids=["01000000"])
    vectors = np.asarray(result["embedding"].to_list(), dtype=np.float32)
    assert vectors.dtype == np.float32


def test_join_with_documents_preserves_order_and_drops_missing() -> None:
    documents = pl.DataFrame({"item_id": ["1", "2", "3"], "gtl_id": ["01000000"] * 3})
    embeddings = pl.DataFrame(
        {
            "item_id": ["3", "1"],  # out of order, missing "2"
            "embedding": [[0.1, 0.2], [0.3, 0.4]],
            "zero_norm": [False, False],
        }
    )

    joined = join_with_documents(embeddings, documents)

    assert joined["item_id"].to_list() == ["1", "3"]  # documents' order


def test_validate_embeddings_passes_for_unit_norm_vectors() -> None:
    data = pl.DataFrame(
        {
            "item_id": ["1", "2"],
            "embedding": [[1.0, 0.0], [0.0, 1.0]],
            "zero_norm": [False, False],
        }
    )
    validate_embeddings(data, expected_item_ids=["1", "2"])


def test_validate_embeddings_raises_on_order_mismatch() -> None:
    data = pl.DataFrame(
        {
            "item_id": ["1", "2"],
            "embedding": [[1.0, 0.0], [0.0, 1.0]],
            "zero_norm": [False, False],
        }
    )
    with pytest.raises(ValueError, match="does not match"):
        validate_embeddings(data, expected_item_ids=["2", "1"])


def test_validate_embeddings_raises_on_length_mismatch() -> None:
    data = pl.DataFrame(
        {"item_id": ["1"], "embedding": [[1.0, 0.0]], "zero_norm": [False]}
    )
    with pytest.raises(ValueError, match="does not match"):
        validate_embeddings(data, expected_item_ids=["1", "2"])


def test_validate_embeddings_raises_on_bad_norm() -> None:
    data = pl.DataFrame(
        {"item_id": ["1"], "embedding": [[2.0, 0.0]], "zero_norm": [False]}
    )
    with pytest.raises(ValueError, match="not unit-norm"):
        validate_embeddings(data, expected_item_ids=["1"])


def test_validate_embeddings_ignores_zero_norm_rows() -> None:
    data = pl.DataFrame(
        {"item_id": ["1"], "embedding": [[0.0, 0.0]], "zero_norm": [True]}
    )
    validate_embeddings(data, expected_item_ids=["1"])
