import numpy as np
import polars as pl

from src.labelling.ctfidf import (
    OUTLIER_LABEL,
    build_count_matrix,
    build_label,
    compute_ctfidf,
    compute_idf,
    select_corpus_vocabulary,
    summarize_topics,
    top_words_per_topic,
)

THEMES = {
    "0": ["recette", "cuisine", "plat", "ingredient"],
    "1": ["voyage", "pays", "avion", "valise"],
    "2": ["sport", "match", "equipe", "ballon"],
}
FILLER = ["livre", "auteur"]


def test_select_corpus_vocabulary_excludes_stop_words_and_low_df() -> None:
    vocabulary = pl.DataFrame(
        {
            "term": ["roman", "stop1", "rare"],
            "df_total": [100, 50, 2],
            "is_stop_word": [False, True, False],
        }
    )
    assert select_corpus_vocabulary(vocabulary, min_df=15, max_features=50000) == [
        "roman"
    ]


def test_select_corpus_vocabulary_keeps_top_max_features_by_df() -> None:
    vocabulary = pl.DataFrame(
        {
            "term": ["a", "b", "c"],
            "df_total": [30, 20, 10],
            "is_stop_word": [False, False, False],
        }
    )
    assert select_corpus_vocabulary(vocabulary, min_df=15, max_features=1) == ["a"]


def _documents_for_three_themes() -> tuple[pl.DataFrame, list[str]]:
    """3 themes, each with its own word pool, plus shared filler words --
    the synthetic-data shape spec section 9 asks for.
    """
    item_ids: list[str] = []
    gtl_ids: list[str] = []
    topic_uuids: list[str] = []
    is_outliers: list[bool] = []
    documents: list[str] = []
    for topic_uuid, words in THEMES.items():
        for i in range(20):
            item_ids.append(f"{topic_uuid}-{i}")
            gtl_ids.append("01000000" if i % 2 == 0 else "02000000")
            topic_uuids.append(topic_uuid)
            is_outliers.append(False)
            documents.append(" ".join(words + FILLER))

    df = pl.DataFrame(
        {
            "item_id": item_ids,
            "gtl_id": gtl_ids,
            "topic_uuid": topic_uuids,
            "is_outlier": is_outliers,
            "document": documents,
        }
    )
    vocabulary = sorted({w for words in THEMES.values() for w in words} | set(FILLER))
    return df, vocabulary


def test_ctfidf_top_words_come_from_own_theme_pool() -> None:
    documents, vocabulary = _documents_for_three_themes()
    topic_ids = documents["topic_uuid"].unique().to_list()
    count_matrix = build_count_matrix(documents, topic_ids, vocabulary)
    presence_matrix = build_count_matrix(documents, topic_ids, vocabulary, binary=True)
    scores = compute_ctfidf(count_matrix)
    top_words = top_words_per_topic(
        scores, count_matrix, presence_matrix.toarray(), vocabulary, top_n_words=4
    )

    for topic_index, topic_uuid in enumerate(topic_ids):
        top_terms = {word["term"] for word in top_words[topic_index]}
        assert top_terms
        assert top_terms <= set(THEMES[topic_uuid])
        assert "livre" not in top_terms
        assert "auteur" not in top_terms


def test_ctfidf_counts_and_df_consistent_with_topic_size() -> None:
    documents, vocabulary = _documents_for_three_themes()
    topic_ids = documents["topic_uuid"].unique().to_list()
    count_matrix = build_count_matrix(documents, topic_ids, vocabulary)
    presence_matrix = build_count_matrix(documents, topic_ids, vocabulary, binary=True)
    scores = compute_ctfidf(count_matrix)
    top_words = top_words_per_topic(
        scores, count_matrix, presence_matrix.toarray(), vocabulary, top_n_words=10
    )

    for words in top_words:
        for word in words:
            assert word["count"] >= word["df"] > 0
            assert word["df"] <= 20


def test_compute_idf_downweights_words_common_to_every_topic() -> None:
    documents, vocabulary = _documents_for_three_themes()
    topic_ids = documents["topic_uuid"].unique().to_list()
    count_matrix = build_count_matrix(documents, topic_ids, vocabulary)
    idf = compute_idf(count_matrix)
    word_index = {term: i for i, term in enumerate(vocabulary)}
    assert idf[word_index["livre"]] < idf[word_index["cuisine"]]


def test_compute_ctfidf_reduce_frequent_words_toggle_changes_scores() -> None:
    documents, vocabulary = _documents_for_three_themes()
    topic_ids = documents["topic_uuid"].unique().to_list()
    count_matrix = build_count_matrix(documents, topic_ids, vocabulary)
    with_sqrt = compute_ctfidf(count_matrix, reduce_frequent_words=True)
    without_sqrt = compute_ctfidf(count_matrix, reduce_frequent_words=False)
    assert not np.allclose(with_sqrt, without_sqrt)


def test_build_label_joins_top_words_with_underscore() -> None:
    top_words = [{"term": "roman"}, {"term": "histoire"}, {"term": "aimer"}]
    assert build_label(top_words, label_words=2, is_outlier=False) == "roman_histoire"


def test_build_label_outlier_group() -> None:
    assert build_label([], label_words=2, is_outlier=True) == OUTLIER_LABEL


def _summarize(documents: pl.DataFrame, vocabulary: list[str]) -> pl.DataFrame:
    return summarize_topics(
        documents,
        vocabulary,
        split_level=1,
        corpus_id="01000000",
        corpus_label="Test",
        corpus_depth=1,
        top_n_words=4,
        label_words=2,
        reduce_frequent_words=True,
    )


def test_summarize_topics_gtl_breakdown_sums_to_topic_size() -> None:
    documents, vocabulary = _documents_for_three_themes()
    topics = _summarize(documents, vocabulary)
    for row in topics.to_dicts():
        assert sum(g["n_items"] for g in row["gtl_breakdown"]) == row["n_items"]


def test_summarize_topics_share_of_corpus_sums_to_one() -> None:
    documents, vocabulary = _documents_for_three_themes()
    topics = _summarize(documents, vocabulary)
    assert np.isclose(topics["share_of_corpus"].sum(), 1.0)


def test_summarize_topics_labels_outlier_group() -> None:
    documents, vocabulary = _documents_for_three_themes()
    documents = documents.with_columns(
        (pl.col("topic_uuid") == "0").alias("is_outlier")
    )
    topics = _summarize(documents, vocabulary)
    outlier_row = topics.filter(pl.col("topic_uuid") == "0").row(0, named=True)
    assert outlier_row["label"] == OUTLIER_LABEL
