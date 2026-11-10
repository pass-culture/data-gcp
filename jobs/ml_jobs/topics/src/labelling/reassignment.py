"""Optional outlier reassignment (spec section 7.4, off by default --
``reassign_outliers`` in the config). BERTopic's own rule accepts a single
shared word, which attaches items on weak evidence; this requires at least
``min_shared_words``.
"""

import numpy as np


def transform_outlier_vector(
    document: str, vocabulary: list[str], idf: np.ndarray
) -> np.ndarray:
    """The same ``sqrt(tf) * idf`` transform (not re-fit) applied to one
    outlier's count vector.
    """
    term_index = {term: index for index, term in enumerate(vocabulary)}
    counts = np.zeros(len(vocabulary))
    for word in (document or "").split():
        col = term_index.get(word)
        if col is not None:
            counts[col] += 1
    total = counts.sum()
    if total == 0:
        return counts
    tf = np.sqrt(counts / total)
    return tf * idf


def cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    norm_a = np.linalg.norm(a)
    norm_b = np.linalg.norm(b)
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(np.dot(a, b) / (norm_a * norm_b))


def find_reassignment(
    outlier_vector: np.ndarray,
    topic_vectors: np.ndarray,
    topic_ids: list[str],
    *,
    min_shared_words: int,
) -> str | None:
    """The ``topic_uuid`` to move an outlier to, or ``None`` to leave it an
    outlier: the highest-cosine-similarity topic, only if that similarity is
    > 0 and at least ``min_shared_words`` words have a positive score in
    both the outlier and the topic.
    """
    best_topic_id: str | None = None
    best_similarity = 0.0
    for topic_id, topic_vector in zip(topic_ids, topic_vectors, strict=True):
        similarity = cosine_similarity(outlier_vector, topic_vector)
        if similarity <= best_similarity or similarity <= 0:
            continue
        shared = int(np.sum((outlier_vector > 0) & (topic_vector > 0)))
        if shared >= min_shared_words:
            best_similarity = similarity
            best_topic_id = topic_id
    return best_topic_id
