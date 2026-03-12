---
title: Item Embedding
description: Description of the `ml_feat__item_embedding_refactor` table.
---

{% docs     description__ml_feat__item_embedding_refactor %}

# View: Two Tower Last Item Embedding

The `ml_feat__item_embedding_refactor` table contains item embeddings produced by embedding-gemma-300m. Each item has:

- a unique hash that hashes the content of its metadata.
- multiple embedding columns that are defined in the item_embedding microservice config.

{% enddocs %}

## Table description

{% docs table__ml_feat__item_embedding_refactor %}{% enddocs %}
