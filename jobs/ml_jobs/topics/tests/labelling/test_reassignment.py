import numpy as np
import pytest

from src.labelling.reassignment import (
    cosine_similarity,
    find_reassignment,
    transform_outlier_vector,
)


def test_cosine_similarity_identical_vectors_is_one() -> None:
    vector = np.array([1.0, 2.0, 3.0])
    assert cosine_similarity(vector, vector) == pytest.approx(1.0)


def test_cosine_similarity_zero_vector_is_zero() -> None:
    assert cosine_similarity(np.zeros(3), np.array([1.0, 2.0, 3.0])) == 0.0


def test_cosine_similarity_orthogonal_vectors_is_zero() -> None:
    similarity = cosine_similarity(np.array([1.0, 0.0]), np.array([0.0, 1.0]))
    assert similarity == pytest.approx(0.0)


def test_transform_outlier_vector_empty_document_returns_zeros() -> None:
    result = transform_outlier_vector("", ["a", "b"], np.array([1.0, 2.0]))
    assert np.all(result == 0)


def test_transform_outlier_vector_zeroed_by_zero_idf() -> None:
    result = transform_outlier_vector("a a b", ["a", "b"], np.array([2.0, 0.0]))
    assert result[1] == 0.0
    assert result[0] > 0


def test_find_reassignment_picks_highest_similarity_above_threshold() -> None:
    outlier_vector = np.array([1.0, 1.0, 0.0, 0.0])
    topic_vectors = np.array(
        [
            [1.0, 1.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 1.0],
        ]
    )
    result = find_reassignment(
        outlier_vector, topic_vectors, ["t0", "t1"], min_shared_words=2
    )
    assert result == "t0"


def test_find_reassignment_stays_outlier_below_min_shared_words() -> None:
    outlier_vector = np.array([1.0, 1.0, 0.0, 0.0])
    topic_vectors = np.array([[1.0, 0.0, 0.0, 0.0]])
    result = find_reassignment(
        outlier_vector, topic_vectors, ["t0"], min_shared_words=2
    )
    assert result is None


def test_find_reassignment_stays_outlier_when_similarity_is_zero() -> None:
    outlier_vector = np.array([1.0, 0.0])
    topic_vectors = np.array([[0.0, 1.0]])
    result = find_reassignment(
        outlier_vector, topic_vectors, ["t0"], min_shared_words=0
    )
    assert result is None
