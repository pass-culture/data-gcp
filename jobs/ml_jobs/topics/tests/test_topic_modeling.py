import importlib.util

import numpy as np
import pandas as pd
import pytest

from src.topic_modeling import auto_min_cluster_size, fit_topics, summarize_topics

THEMES = {
    "Policier": "enquête meurtre inspecteur crime",
    "Cuisine": "recette cuisine chef saveur",
    "Astronomie": "étoile planète galaxie télescope",
}


def _sample(n_per_theme: int = 120, dim: int = 128):
    rng = np.random.default_rng(0)
    rows, vectors = [], []
    for k, (genre, words) in enumerate(THEMES.items()):
        center = rng.normal(size=dim)
        for i in range(n_per_theme):
            vectors.append(center + 0.05 * rng.normal(size=dim))
            # theme nouns + generic nouns shared by every theme
            rows.append(
                {
                    "item_id": f"{k}-{i}",
                    "offer_name": f"{genre} {i}",
                    "gtl_label_level_1": genre,
                    "nouns": f"{words} livre roman histoire",
                }
            )
    return pd.DataFrame(rows), np.array(vectors, dtype=np.float32)


HAS_CUML = importlib.util.find_spec("cuml") is not None


@pytest.mark.parametrize(
    "device",
    [
        "cpu",
        pytest.param(
            "gpu",
            marks=pytest.mark.skipif(not HAS_CUML, reason="needs `--extra gpu`"),
        ),
    ],
)
def test_clusters_recover_themes_and_ctfidf_labels_them(device):
    items, embeddings = _sample()
    topics, model = fit_topics(items, embeddings, min_cluster_size=30, device=device)
    found = topics[topics["topic"] != -1]
    assert found["topic"].nunique() == 3
    # each topic is a single theme
    assert (found.groupby("topic")["gtl_label_level_1"].nunique() == 1).all()

    summary = summarize_topics(topics, model)
    summary = summary[summary["topic"] != -1]
    for _, row in summary.iterrows():
        top = row["top_nouns"].split()[:4]
        assert set(top) == set(THEMES[row["top_gtl_label_level_1"]].split())
        assert row["top_gtl_label_level_1_share"] == 1.0
    assert summary["n_items"].sum() == len(found)


def test_auto_min_cluster_size_overclusters():
    assert auto_min_cluster_size(30_000, 50) == 200  # 30k / (3 * 50)
    assert auto_min_cluster_size(100, 50) == 5  # floor


def _many_themes(n_themes: int = 8, n_per_theme: int = 60, dim: int = 128):
    rng = np.random.default_rng(1)
    rows, vectors = [], []
    for k in range(n_themes):
        center = rng.normal(size=dim)
        words = " ".join(f"mot{chr(97 + k)}{j}" for j in range(4))
        for i in range(n_per_theme):
            vectors.append(center + 0.05 * rng.normal(size=dim))
            rows.append({"item_id": f"{k}-{i}", "offer_name": f"t{k}", "nouns": words})
    return pd.DataFrame(rows), np.array(vectors, dtype=np.float32)


@pytest.mark.parametrize(
    "device",
    [
        "cpu",
        pytest.param(
            "gpu",
            marks=pytest.mark.skipif(not HAS_CUML, reason="needs `--extra gpu`"),
        ),
    ],
)
def test_n_topics_merges_down_to_about_the_target(device):
    items, embeddings = _many_themes()
    free, _ = fit_topics(items, embeddings, min_cluster_size=20, device=device)
    assert free.loc[free["topic"] != -1, "topic"].nunique() == 8

    topics, _ = fit_topics(items, embeddings, n_topics=4, device=device)
    found = topics.loc[topics["topic"] != -1, "topic"].nunique()
    assert 4 <= found <= 5  # +1 when HDBSCAN leaves no outliers
    # merging keeps whole themes together
    assert (topics.groupby("offer_name")["topic"].nunique() == 1).all()
