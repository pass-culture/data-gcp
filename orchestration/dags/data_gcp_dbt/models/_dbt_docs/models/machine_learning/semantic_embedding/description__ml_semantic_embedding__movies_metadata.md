---
title: Movies Metadata Embedding
description: Description of the `ml_semantic_embedding__movies_metadata` table.
---

{% docs description__ml_semantic_embedding__movies_metadata %}

# Table: Movies Metadata Embedding

The `ml_semantic_embedding__movies_metadata` table contains the final embeddings of movie items' metadata. The column `mlflow_run_id` links to the run_id on MLflow where we logged all information about the embedding prompt, features and preprocessing. The column `embedding_model` contains the embedding model used to embed the items.

The table is incrementally merged on `item_id`: only items whose `content_hash` changed are re-embedded, so existing embeddings are preserved across runs.

{% enddocs %}

## Table description

{% docs table__ml_semantic_embedding__movies_metadata %}{% enddocs %}
