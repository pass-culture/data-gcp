# Table: Books Metadata Embedding

The `ml_semantic_embedding__books_metadata` table contains the final embeddings of book items' metadata. The column `mlflow_run_id` links to the run_id on MLflow where we logged all information about the embedding prompt, features and preprocessing. The column `embedding_model` contains the embedding model used to embed the items.

The table is incrementally merged on `item_id`: only items whose `content_hash` changed are re-embedded, so existing embeddings are preserved across runs.

## Table description

| name                     | data_type | description                                                                                                                                             |
| ------------------------ | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| item_id                  |           | Identifier of the work (item) the offer belongs to, used internally by the data science team; several offers of the same book or film share an item_id. |
| content_hash             |           | Hash of item's metadata.                                                                                                                                |
| books_metadata_embedding |           | Embedding of the book item's metadata.                                                                                                                  |
| mlflow_run_id            |           | Model mlflow run id.                                                                                                                                    |
| embedding_model          |           | Name of the model used to generate the embedding.                                                                                                       |
| embedding_date           |           | Date at which the embedding was generated.                                                                                                              |
