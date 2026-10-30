# Table: All Items Offer Names Embedding 128

The `ml_semantic_embedding__all_items_offer_names_128` table contains the embeddings from `ml_semantic_embedding__all_items_offer_names` truncated to their first 128 dimensions using ARRAY_SLICE function. The column `mlflow_run_id` links to the run_id on MLflow where we log all information about the prompt used to embed items, features added to prompt and preprocessing functions. The column `embedding_model` contains the embedding model used to embed the items.

The vector is **not** L2-normalized. Normalize downstream after truncation if a unit vector is required (MRL truncation breaks the original norm).

## Table description

| name                                | data_type | description                                                                                                                                             |
| ----------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| item_id                             |           | Identifier of the work (item) the offer belongs to, used internally by the data science team; several offers of the same book or film share an item_id. |
| content_hash                        |           | Hash of item's metadata.                                                                                                                                |
| mlflow_run_id                       |           | Model mlflow run id.                                                                                                                                    |
| embedding_model                     |           | Name of the model used to generate the embedding.                                                                                                       |
| embedding_date                      |           | Date at which the embedding was generated.                                                                                                              |
| all_items_offer_names_embedding_128 |           | First 128 dimensions of the item's offer name embedding, truncated with Matryoshka Representation Learning (MRL). NOT NORMALIZED!                       |
