## Two towers model

🛠️ UNDER CONSTRUCTION

## Embeddings historization
After training and evaluating the two towers model, we save user and items embeddings in two partitionned BigQuery table in `raw_<env>` dataset. `<env>` is **dev**, **stg** or **prod**. Each partition corresponds one model i.e. one embedding for each user or item. The idea is to have a history of how users and items embeddings change after each training. The two tables are:
* `two_tower_user_embedding` : contains all information about the model (experiment_id, run_id, etc) and the vector embedding (64,) for each user.
* `two_tower_item_embedding`: contains all information about the model (experiment_id, run_id, etc) and the vector embedding (64,) for each item.

**-> How the historization task was implemented?**
* `upload_embeddings_to_bq.py`: contains the main script of the historization task, here is the logic of the script:
    1. Load trained model of the latest MLFlow experiment which contains the url to the GCS bucket that contain the model weights
    2. Extract  user (resp item) id list and user (resp item) embedding from model layers.
    3. Create two dataframes one for users and another for item which contains all data we need to upload
    4. Through a BigQuery Client, create a schema for apartionned table. Specify data types for each column. For the embedding column, specify a datatyepe of type `FLOAT` with a `mode='repeated'`  which garanttess that the column is of type `ARRAY` and compatible with GCP functions to create `VECTOR INDEX` and later run a `VECTOR SEARCH` function over tis column (which runs faster if you create a `VECTOR INDEX` on your `ARRAY` column, see doc for more information).
    5. Export tables to BigQuery

* Added a task to `algo_training_two_tower.py` DAG called `upload_embeddings_to_bq` which executes after evaluation and calles the previous script.

To run a VECTOR SEARCH function, please refer to this [Notion page ](https://www.notion.so/passcultureapp/Vector-Search-with-BigQuery-81952e07282c41f1a3640aa8bcbe0c3c?pvs=4 )and/or [GCP documentation] (https://cloud.google.com/bigquery/docs/vector-index?hl=fr)
