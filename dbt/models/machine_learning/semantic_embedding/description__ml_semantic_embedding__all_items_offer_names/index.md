# Table: All Items Metadata Embedding

The `ml_semantic_embedding__all_items_offer_names` table contains the semantic embeddings of all the offer names of the catalog items. The column `mlflow_run_id` links to the run_id on MLflow where we log all information about the prompt used to embed items, features added to prompt and preprocessing functions. The column `embedding_model` contains the embedding model used to embed the items.

The table is incrementally merged on `item_id`: only items whose `content_hash` changed are re-embedded, so existing embeddings are preserved across runs.

## Table description

| name                            | data_type | description                                                                                                                                             |
| ------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| item_id                         |           | Identifier of the work (item) the offer belongs to, used internally by the data science team; several offers of the same book or film share an item_id. |
| content_hash                    |           | Hash of item's metadata.                                                                                                                                |
| all_items_offer_names_embedding |           | Embedding of the offer name associated with the item.                                                                                                   |
| mlflow_run_id                   |           | Model mlflow run id.                                                                                                                                    |
| embedding_model                 |           | Name of the model used to generate the embedding.                                                                                                       |
| embedding_date                  |           | Date at which the embedding was generated.                                                                                                              |
