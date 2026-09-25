---
title: All Items Metadata Embedding 128
description: Description of the `ml_semantic_embedding__all_items_metadata_128` table.
---

{% docs description__ml_semantic_embedding__all_items_metadata_128 %}

# Table: All Items Metadata Embedding 128

The `ml_semantic_embedding__all_items_metadata_128` table contains the embeddings from `ml_semantic_embedding__all_items_metadata` truncated to their first 128 dimensions using ARRAY_SLICE function. The column `mlflow_run_id` links to the run_id on MLflow where we log all information about the prompt used to embed items, features added to prompt and preprocessing functions. The column `embedding_model` contains the embedding model used to embed the items.

The vector is **not** L2-normalized. Normalize downstream after truncation if a unit vector is required (MRL truncation breaks the original norm).

{% enddocs %}

## Table description

{% docs table__ml_semantic_embedding__all_items_metadata_128 %}{% enddocs %}
