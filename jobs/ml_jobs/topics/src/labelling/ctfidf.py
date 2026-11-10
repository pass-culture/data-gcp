"""c-TF-IDF labelling (spec section 7.4): scores each corpus word per
topic, then picks each topic's top words and label.
"""

import numpy as np
import polars as pl
from scipy import sparse

OUTLIER_LABEL = "Non clusterisés"


def select_corpus_vocabulary(
    vocabulary: pl.DataFrame, min_df: int, max_features: int
) -> list[str]:
    """Registry words (stop words excluded) with ``df_total >= min_df``,
    keeping the ``max_features`` with the highest document frequency.
    """
    eligible = (
        vocabulary.filter(~pl.col("is_stop_word") & (pl.col("df_total") >= min_df))
        .sort("df_total", descending=True)
        .head(max_features)
    )
    return eligible["term"].to_list()


def build_count_matrix(
    documents: pl.DataFrame,
    topic_ids: list[str],
    vocabulary: list[str],
    *,
    binary: bool = False,
) -> sparse.csr_matrix:
    """``C[topic, word]``: for each item, the vocabulary words in its
    document (counted, or just present/absent when ``binary``), summed per
    topic via a sparse topic-membership x item-count product -- not by
    concatenating a topic's documents into one string (spec's explicit
    warning against that at scale).

    ``documents`` needs ``item_id``, ``topic_uuid``, ``document`` (the
    space-joined kept lemmas).
    """
    term_index = {term: index for index, term in enumerate(vocabulary)}
    n_items = documents.height

    rows: list[int] = []
    cols: list[int] = []
    data: list[int] = []
    for item_index, doc in enumerate(documents["document"].to_list()):
        counts: dict[int, int] = {}
        for word in (doc or "").split():
            col = term_index.get(word)
            if col is not None:
                counts[col] = counts.get(col, 0) + 1
        for col, count in counts.items():
            rows.append(item_index)
            cols.append(col)
            data.append(1 if binary else count)
    item_word_matrix = sparse.csr_matrix(
        (data, (rows, cols)), shape=(n_items, len(vocabulary))
    )

    topic_index = {topic_id: index for index, topic_id in enumerate(topic_ids)}
    membership_rows = [topic_index[t] for t in documents["topic_uuid"].to_list()]
    membership_cols = list(range(n_items))
    membership = sparse.csr_matrix(
        (np.ones(n_items), (membership_rows, membership_cols)),
        shape=(len(topic_ids), n_items),
    )

    return (membership @ item_word_matrix).tocsr()


def compute_idf(count_matrix: sparse.csr_matrix) -> np.ndarray:
    """``idf = log(A / f + 1)``: ``A`` is the mean topic size (integer
    part), ``f`` is each word's total count across every topic.
    """
    topic_sums = np.asarray(count_matrix.sum(axis=1)).flatten()
    corpus_average_size = int(topic_sums.mean())
    word_totals = np.asarray(count_matrix.sum(axis=0)).flatten()
    safe_totals = np.where(word_totals == 0, 1, word_totals)
    return np.log(corpus_average_size / safe_totals + 1)


def compute_ctfidf(
    count_matrix: sparse.csr_matrix, *, reduce_frequent_words: bool = True
) -> np.ndarray:
    """``score = tf * idf``, ``tf`` L1-normalized per topic then ``sqrt``'d
    when ``reduce_frequent_words`` (damps words common across every topic).
    """
    idf = compute_idf(count_matrix)
    dense = count_matrix.toarray().astype(np.float64)
    row_sums = dense.sum(axis=1, keepdims=True)
    tf = np.divide(dense, row_sums, out=np.zeros_like(dense), where=row_sums != 0)
    if reduce_frequent_words:
        tf = np.sqrt(tf)
    return tf * idf


def top_words_per_topic(
    scores: np.ndarray,
    count_matrix: sparse.csr_matrix,
    topic_df_matrix: np.ndarray,
    vocabulary: list[str],
    top_n_words: int,
) -> list[list[dict[str, object]]]:
    """For each topic, the ``top_n_words`` highest-scoring words with a
    positive score, each with its score, occurrence count, and document
    frequency within the topic.
    """
    dense_counts = count_matrix.toarray()
    results: list[list[dict[str, object]]] = []
    for topic_index in range(scores.shape[0]):
        topic_scores = scores[topic_index]
        order = np.argsort(-topic_scores)
        words: list[dict[str, object]] = []
        for word_index in order:
            if len(words) >= top_n_words:
                break
            score = topic_scores[word_index]
            if score <= 0:
                break
            words.append(
                {
                    "term": vocabulary[word_index],
                    "score": float(score),
                    "count": int(dense_counts[topic_index, word_index]),
                    "df": int(topic_df_matrix[topic_index, word_index]),
                }
            )
        results.append(words)
    return results


def build_label(
    top_words: list[dict[str, object]], label_words: int, *, is_outlier: bool
) -> str:
    if is_outlier:
        return OUTLIER_LABEL
    return "_".join(str(word["term"]) for word in top_words[:label_words])


def summarize_topics(
    documents: pl.DataFrame,
    vocabulary: list[str],
    *,
    split_level: int,
    corpus_id: str,
    corpus_label: str,
    corpus_depth: int,
    top_n_words: int,
    label_words: int,
    reduce_frequent_words: bool,
) -> pl.DataFrame:
    """One row per topic (the outlier group included): n_items,
    share_of_corpus, top_words, gtl_breakdown, label.

    ``documents`` needs ``item_id``, ``gtl_id``, ``topic_uuid``, ``document``,
    ``is_outlier``.
    """
    topic_ids = documents["topic_uuid"].unique().to_list()
    count_matrix = build_count_matrix(documents, topic_ids, vocabulary)
    presence_matrix = build_count_matrix(documents, topic_ids, vocabulary, binary=True)
    scores = compute_ctfidf(count_matrix, reduce_frequent_words=reduce_frequent_words)
    topic_df_matrix = presence_matrix.toarray()
    top_words_by_topic = top_words_per_topic(
        scores, count_matrix, topic_df_matrix, vocabulary, top_n_words
    )

    total_items = documents.height
    rows: list[dict[str, object]] = []
    for topic_index, topic_uuid in enumerate(topic_ids):
        topic_docs = documents.filter(pl.col("topic_uuid") == topic_uuid)
        is_outlier = bool(topic_docs["is_outlier"][0])
        top_words = top_words_by_topic[topic_index]
        gtl_breakdown = (
            topic_docs.group_by("gtl_id").agg(pl.len().alias("n_items")).to_dicts()
        )
        rows.append(
            {
                "topic_uuid": topic_uuid,
                "split_level": split_level,
                "corpus_id": corpus_id,
                "corpus_label": corpus_label,
                "corpus_depth": corpus_depth,
                "is_outlier": is_outlier,
                "n_items": topic_docs.height,
                "share_of_corpus": topic_docs.height / total_items
                if total_items
                else 0.0,
                "label": build_label(top_words, label_words, is_outlier=is_outlier),
                "top_words": top_words,
                "gtl_breakdown": gtl_breakdown,
            }
        )
    return pl.DataFrame(rows)
