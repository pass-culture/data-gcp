---
title: Item Embedding 128
description: Description of the `ml_feat__item_embedding_refactor_128` table.
---

{% docs description__ml_feat__item_embedding_refactor_128 %}

# Table Item Embedding 128

The `ml_feat__item_embedding_refactor_128` table contains the item embeddings from `ml_feat__item_embedding_refactor` truncated to their first 128 dimensions using Matryoshka Representation Learning (MRL).

Each item has:

- `content_hash`: hash of the item's metadata, used to refresh only changed items.
- `semantic_content_128`: the first 128 dimensions of the item embedding, in original
  order. The vector is **not** L2-normalized here. Normalize downstream after
  truncation if a unit vector is required (MRL truncation breaks the original norm).

{% enddocs %}

## Table description

{% docs table__ml_feat__item_embedding_refactor_128 %}{% enddocs %}
