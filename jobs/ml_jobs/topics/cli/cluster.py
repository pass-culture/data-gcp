import typer

from cli import options
from src.config import EMBEDDING_DATASET
from src.storage import read_table, write_table


def cluster(
    *,
    enriched_path: str = typer.Option(
        "data/nlp_enriched", help="Enriched sample from preprocess"
    ),
    project: str = options.PROJECT,
    embedding_dataset: str = typer.Option(
        EMBEDDING_DATASET,
        help="BigQuery dataset holding books_metadata (env: ENV_SHORT_NAME)",
    ),
    cache_dir: str = options.CACHE_DIR,
    n_topics: int | None = typer.Option(
        None,
        min=1,
        help="Approximate number of topics wanted: over-cluster, then merge "
        "similar topics down to it (default: as many as HDBSCAN finds)",
    ),
    min_cluster_size: int | None = typer.Option(
        None,
        min=2,
        help="HDBSCAN: smallest group of items that forms a topic (default: 30, "
        "or derived from --n-topics)",
    ),
    n_neighbors: int = typer.Option(
        15, min=2, help="UMAP: neighbourhood size (higher = broader topics)"
    ),
    n_components: int = typer.Option(5, min=2, help="UMAP: output dimensions"),
    min_df: int = typer.Option(
        2, min=1, help="c-TF-IDF: ignore nouns found in fewer items"
    ),
    seed: int = typer.Option(42, help="UMAP random seed"),
    device: str = typer.Option(
        "cpu", help="cpu, or gpu (RAPIDS cuML: `uv sync --extra gpu`)"
    ),
    out: str = typer.Option(
        "data/topics", help="Per-item topics: writes <stem>.parquet + <stem>.csv"
    ),
    summary_out: str = typer.Option(
        "data/topic_summary",
        help="Per-topic summary: writes <stem>.parquet + <stem>.csv",
    ),
) -> None:
    """Cluster the enriched sample on its book embeddings (UMAP + HDBSCAN, as
    in BERTopic) and label clusters with c-TF-IDF over the lemmatized nouns."""
    from src.embeddings import EMBEDDING_COL, load_embeddings, to_matrix
    from src.topic_modeling import fit_topics, summarize_topics

    items = read_table(enriched_path)
    embeddings = load_embeddings(
        items["item_id"].tolist(), cache_dir, project, embedding_dataset
    )
    items = items.merge(embeddings, on="item_id", how="inner")
    topics, model = fit_topics(
        items.drop(columns=EMBEDDING_COL),
        to_matrix(items[EMBEDDING_COL]),
        n_neighbors=n_neighbors,
        n_components=n_components,
        n_topics=n_topics,
        min_cluster_size=min_cluster_size,
        min_df=min_df,
        seed=seed,
        device=device,
    )
    write_table(topics, out)
    write_table(summarize_topics(topics, model), summary_out)
