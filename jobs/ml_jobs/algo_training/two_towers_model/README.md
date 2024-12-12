## Two towers model

🛠️ UNDER CONSTRUCTION

## Embeddings historization
After training and evaluating the two towers model, we save user and items embeddings in two partitionned BigQuery table in `raw_<env>` dataset. `<env>` is **dev**, **stg** or **prod**. Each partition corresponds one model i.e. one embedding for each user or item. The idea is to have a history of how users and items embeddings change after each training. The two tables are:
* `two_tower_user_embedding` : contains all information about the model (experiment_id, run_id, etc) and the vector embedding (64,) for each user.
* `two_tower_item_embedding`: contains all information about the model (experiment_id, run_id, etc) and the vector embedding (64,) for each item.

##### `upload_embeddings_to_bq.py` overview:

1. Load trained model of the latest MLFlow experiment which contains the url to the GCS bucket that contains the model weights.
2. Extract user (resp item) id list and user (resp item) embedding from model layers.
3. Create two dataframes one for users and another for items, each will contain all data we need to upload.
4. Use BigQuery Client to create a schema for a partionned table and specify requiered data type for each column.
5. For the data type of the embedding column, the datatype should be of type `FLOAT` with a `mode='repeated'` which guarantees that :
    * the embedding column is of type `ARRAY`
    * the embedding column is compatible with GCP functions to create `VECTOR INDEX` and later run a `VECTOR SEARCH` function over the column (which runs faster if you create a `VECTOR INDEX` on your `ARRAY` column, see doc for more information).
6. Export tables to BigQuery


For more info on how to run a `VECTOR SEARCH` function, please refer to this [Notion page ](https://www.notion.so/passcultureapp/Vector-Search-with-BigQuery-81952e07282c41f1a3640aa8bcbe0c3c?pvs=4 )and/or [GCP documentation](https://cloud.google.com/bigquery/docs/vector-index?hl=fr)
