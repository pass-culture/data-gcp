---
title: All Items offer names Embedding
description: Description of the `ml_semantic_embedding__all_items_offer_names` table.
---

{% docs description__ml_semantic_embedding__all_items_offer_names %}

# Table: All Items Metadata Embedding

The `ml_semantic_embedding__all_items_offer_names` table contains the semantic embeddings of all the offer names of the catalog items. The column `mlflow_run_id` links to the run_id on MLflow where we log all information about the prompt used to embed items, features added to prompt and preprocessing functions. The column `embedding_model` contains the embedding model used to embed the items.

The table is incrementally merged on `item_id`: only items whose `content_hash` changed are re-embedded, so existing embeddings are preserved across runs.

{% enddocs %}

## Table description

{% docs table__ml_semantic_embedding__all_items_offer_names %}{% enddocs %}
