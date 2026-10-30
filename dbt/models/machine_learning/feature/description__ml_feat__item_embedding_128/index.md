# Table Item Embedding 128

The `ml_feat__item_embedding_refactor_128` table contains the item embeddings from `ml_feat__item_embedding_refactor` truncated to their first 128 dimensions using Matryoshka Representation Learning (MRL).

Each item has:

- `content_hash`: hash of the item's metadata, used to refresh only changed items.
- `semantic_content_128`: the first 128 dimensions of the item embedding, in original order. The vector is **not** L2-normalized here. Normalize downstream after truncation if a unit vector is required (MRL truncation breaks the original norm).

## Table description

| name                 | data_type | description                                                                                                                                                                                 |
| -------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| item_id              | STRING    | Identifier of the work (item) the offer belongs to, used internally by the data science team; several offers of the same book or film share an item_id.                                     |
| content_hash         | STRING    | Hash of item's metadata.                                                                                                                                                                    |
| semantic_content_128 | ARRAY     | Truncated embedding of item's metadata with "document" prompt name of size 128. Encoded with embedding-gemma-300m model. Truncated with Matryoshka Representation Learning. NOT NORMALIZED! |
