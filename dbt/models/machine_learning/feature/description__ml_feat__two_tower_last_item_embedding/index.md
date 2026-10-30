# View: Two Tower Last Item Embedding

The `ml_feat__two_tower_last_item_embedding` view contains the item embeddings of the **last** Two Tower model training.

## Table description

| name                   | data_type | description                                                                                                                                             |
| ---------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| item_id                | STRING    | Identifier of the work (item) the offer belongs to, used internally by the data science team; several offers of the same book or film share an item_id. |
| item_embedding         | ARRAY     | Two tower model item embedding.                                                                                                                         |
| train_date             | DATE      | Model train date.                                                                                                                                       |
| mlflow_run_id          | STRING    | Model mlflow run id.                                                                                                                                    |
| mlflow_experiment_name | STRING    | Model mlflow experiment name.                                                                                                                           |
| mlflow_run_name        | STRING    | Model mlflow run name.                                                                                                                                  |
