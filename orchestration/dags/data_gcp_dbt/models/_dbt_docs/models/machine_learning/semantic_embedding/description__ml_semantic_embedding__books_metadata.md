---
title: Books Metadata Embedding
description: Description of the `ml_semantic_embedding__books_metadata` table.
---

{% docs description__ml_semantic_embedding__books_metadata %}

# Table: Books Metadata Embedding

The `ml_semantic_embedding__books_metadata` table contains the final embeddings of book items' metadata. The column `mlflow_run_id` links to the run_id on MLflow where we logged all information about the embedding prompt, features and preprocessing. The column `embedding_model` contains the embedding model used to embed the items.

The table is incrementally merged on `item_id`: only items whose `content_hash` changed are re-embedded, so existing embeddings are preserved across runs.

{% enddocs %}

## Table description

{% docs table__ml_semantic_embedding__books_metadata %}{% enddocs %}
