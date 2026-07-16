# View: Two Tower Last User Embedding

The `ml_feat__two_tower_last_user_embedding` view contains the user embeddings of the **last** Two Tower model training.

## Table description

| name                   | data_type | description                     |
| ---------------------- | --------- | ------------------------------- |
| user_id                | STRING    | Unique identifier for a user.   |
| user_embedding         | ARRAY     | Two tower model user embedding. |
| train_date             | DATE      | Model train date.               |
| mlflow_run_id          | STRING    | Model mlflow run id.            |
| mlflow_experiment_name | STRING    | Model mlflow experiment name.   |
| mlflow_run_name        | STRING    | Model mlflow run name.          |
