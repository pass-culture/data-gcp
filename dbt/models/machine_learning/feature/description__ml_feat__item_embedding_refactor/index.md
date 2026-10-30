# Table Item Embedding Refactor

The `ml_feat__item_embedding_refactor` table contains item embeddings produced by embedding-gemma-300m. Each item has:

- a content hash of the item's metadata.
- embedding columns that are defined in the item_embedding microservice config.

## Table description

| name             | data_type | description                                                                                                                                             |
| ---------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| item_id          | STRING    | Identifier of the work (item) the offer belongs to, used internally by the data science team; several offers of the same book or film share an item_id. |
| content_hash     | STRING    | Hash of item's metadata.                                                                                                                                |
| semantic_content | ARRAY     | Embedding of item's metadata with "document" prompt name. Encoded with embedding-gemma-300m model.                                                      |
