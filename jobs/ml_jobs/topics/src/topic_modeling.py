"""
BERTopic-style clustering of the sampled books, labelled with class-based
TF-IDF (c-TF-IDF) over their lemmatized nouns.

Pipeline
--------
1. Embeddings : precomputed EmbeddingGemma vectors (see src/embeddings.py),
                not re-encoded here -- BERTopic gets them via `embeddings=`.
2. UMAP       : reduce to a few dimensions (cosine metric) so that density
                clustering works; HDBSCAN degrades in high dimensions.
3. HDBSCAN    : density clusters; items in no dense region get topic -1
                (outliers) instead of being forced into a cluster.
4. c-TF-IDF   : each cluster's nouns are concatenated into one "class
                document"; TF-IDF across those class documents surfaces the
                nouns specific to each cluster. reduce_frequent_words damps
                nouns frequent in every cluster ("livre", "roman", ...).

Caveat: the embedded text includes the GTL genres ("Genres: {gtl_concat}" in
the item_embedding prompt), so clusters partly re-discover the GTL
hierarchy -- summarize_topics reports each topic's dominant GTL labels to
make that visible.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
from loguru import logger

if TYPE_CHECKING:
    import numpy as np

# `nouns` is already tokenized (space-joined lemmas): a token is any
# non-space run containing a letter, which drops stray "-" / digits.
NOUN_TOKEN_PATTERN = r"[^\s]*[^\W\d_][^\s]*"

OUTLIER_TOPIC = -1

DEFAULT_MIN_CLUSTER_SIZE = 30

# With a target number of topics, HDBSCAN must first find *more* clusters
# than the target, which BERTopic then merges down: aim for roughly this many
# times the target.
OVERCLUSTER_FACTOR = 3


def auto_min_cluster_size(n_items: int, n_topics: int) -> int:
    """min_cluster_size small enough for HDBSCAN to over-cluster: if items
    were spread evenly over OVERCLUSTER_FACTOR * n_topics clusters, each
    would hold n_items / (OVERCLUSTER_FACTOR * n_topics) of them."""
    return max(5, n_items // (OVERCLUSTER_FACTOR * n_topics))


def noun_vocabulary(docs: list[str], min_df: int = 2) -> list[str]:
    """Nouns found in at least `min_df` *items*. BERTopic fits its vectorizer
    on the per-topic class documents, so a vectorizer-level min_df would
    count topics, not items -- and drop exactly the nouns specific to a
    single topic. Filtering here, then passing a fixed vocabulary, keeps
    min_df's intended meaning."""
    from sklearn.feature_extraction.text import CountVectorizer

    vectorizer = CountVectorizer(
        token_pattern=NOUN_TOKEN_PATTERN, lowercase=False, min_df=min_df
    )
    vectorizer.fit(docs)
    return sorted(vectorizer.vocabulary_)


def cluster_models(device: str = "cpu"):
    """(UMAP, HDBSCAN) classes for `device`: umap-learn + hdbscan on CPU,
    RAPIDS cuML on GPU (install with `uv sync --extra gpu`). Both expose the
    same constructor arguments, and BERTopic supports either."""
    if device == "gpu":
        try:
            from cuml.cluster import HDBSCAN
            from cuml.manifold import UMAP
        except ImportError as e:
            raise ImportError(
                "device='gpu' needs RAPIDS cuML: run `uv sync --extra gpu`"
            ) from e
    elif device == "cpu":
        from hdbscan import HDBSCAN
        from umap import UMAP
    else:
        raise ValueError(f"device must be 'cpu' or 'gpu', got {device!r}")
    return UMAP, HDBSCAN


def build_topic_model(
    vocabulary: list[str],
    *,
    device: str = "cpu",
    n_neighbors: int = 15,
    n_components: int = 5,
    min_cluster_size: int = DEFAULT_MIN_CLUSTER_SIZE,
    n_topics: int | None = None,
    top_n_words: int = 10,
    seed: int = 42,
):
    """BERTopic with BERTopic's default UMAP/HDBSCAN settings except
    min_cluster_size (tuned for samples of ~10-30k items), no embedding
    model (embeddings are passed in), and a vectorizer for pre-lemmatized
    nouns. UMAP/HDBSCAN run on `device` (see cluster_models). With
    `n_topics`, BERTopic merges the most similar topics (c-TF-IDF cosine
    similarity) until about n_topics remain."""
    from bertopic import BERTopic
    from bertopic.vectorizers import ClassTfidfTransformer
    from sklearn.feature_extraction.text import CountVectorizer

    UMAP, HDBSCAN = cluster_models(device)

    return BERTopic(
        embedding_model=None,
        # Not "english" (the default): BERTopic then strips every character
        # outside [A-Za-z0-9 ] before c-TF-IDF, mangling French nouns
        # ("enquête" -> "enqute", "étoile" -> "toile").
        language="multilingual",
        umap_model=UMAP(
            n_neighbors=n_neighbors,
            n_components=n_components,
            min_dist=0.0,
            metric="cosine",
            random_state=seed,
        ),
        hdbscan_model=HDBSCAN(
            min_cluster_size=min_cluster_size,
            metric="euclidean",
            cluster_selection_method="eom",
        ),
        vectorizer_model=CountVectorizer(
            token_pattern=NOUN_TOKEN_PATTERN, lowercase=False, vocabulary=vocabulary
        ),
        ctfidf_model=ClassTfidfTransformer(reduce_frequent_words=True),
        top_n_words=top_n_words,
        calculate_probabilities=False,
        nr_topics=n_topics,
    )


def fit_topics(
    items: pd.DataFrame,
    embeddings: np.ndarray,
    min_df: int = 2,
    n_topics: int | None = None,
    min_cluster_size: int | None = None,
    **model_kwargs,
) -> tuple[pd.DataFrame, object]:
    """Cluster `items` (one row per embedding, same order) and label the
    clusters from their `nouns`. Returns the items with `topic`,
    `topic_probability` and `topic_label` columns, and the fitted model.

    `n_topics` asks for approximately that many topics (outliers excluded):
    HDBSCAN over-clusters (min_cluster_size from auto_min_cluster_size unless
    given), then similar topics are merged down. Merging can only reduce, so
    if HDBSCAN finds fewer clusters than asked, fewer topics come out."""
    if len(items) != len(embeddings):
        raise ValueError(f"{len(items)} items but {len(embeddings)} embeddings")
    if min_cluster_size is None:
        min_cluster_size = (
            auto_min_cluster_size(len(items), n_topics)
            if n_topics
            else DEFAULT_MIN_CLUSTER_SIZE
        )
    docs = items["nouns"].fillna("").tolist()
    model = build_topic_model(
        noun_vocabulary(docs, min_df),
        n_topics=None if n_topics is None else n_topics + 1,  # +1: outliers
        min_cluster_size=min_cluster_size,
        **model_kwargs,
    )
    logger.info(f"Fitting topics on {len(docs)} items ({embeddings.shape[1]}-d)...")
    topics, probs = model.fit_transform(docs, embeddings=embeddings)

    out = items.reset_index(drop=True).copy()
    out["topic"] = topics
    out["topic_probability"] = probs
    labels = model.get_topic_info().set_index("Topic")["Name"]
    out["topic_label"] = out["topic"].map(labels)
    outliers = (out["topic"] == OUTLIER_TOPIC).mean()
    found = len(set(topics) - {OUTLIER_TOPIC})
    logger.info(
        f"{found} topics (min_cluster_size={min_cluster_size}), "
        f"{outliers:.1%} of items are outliers (-1)"
    )
    if n_topics and found < n_topics:
        logger.warning(
            f"Asked for ~{n_topics} topics but HDBSCAN only found {found} "
            f"clusters: lower --min-cluster-size to get more"
        )
    return out, model


def summarize_topics(items: pd.DataFrame, model, n_examples: int = 5) -> pd.DataFrame:
    """One row per topic: size, c-TF-IDF top nouns (with scores), dominant
    GTL labels, and the most typical titles (highest HDBSCAN membership)."""
    import json

    rows = []
    for topic, group in items.groupby("topic"):
        words = model.get_topic(topic) or []
        row = {
            "topic": topic,
            "n_items": len(group),
            "share": len(group) / len(items),
            "top_nouns": " ".join(w for w, _ in words),
            "top_nouns_scores": json.dumps(
                {w: round(float(s), 4) for w, s in words}, ensure_ascii=False
            ),
        }
        for level in ("gtl_label_level_1", "gtl_label_level_2"):
            if level in group:
                counts = group[level].value_counts(normalize=True)
                row[f"top_{level}"] = counts.index[0] if len(counts) else None
                row[f"top_{level}_share"] = counts.iloc[0] if len(counts) else None
        typical = group.nlargest(n_examples, "topic_probability")
        row["example_titles"] = " | ".join(typical["offer_name"].astype(str))
        rows.append(row)
    return pd.DataFrame(rows).sort_values("n_items", ascending=False)
