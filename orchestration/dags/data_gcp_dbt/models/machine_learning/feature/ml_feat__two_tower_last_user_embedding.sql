{{ config(**custom_table_config(materialized="view")) }}

select
    main_table.user_id,
    main_table.user_embedding,
    main_table.train_date,
    main_table.mlflow_run_id,
    main_table.mlflow_experiment_name,
    main_table.mlflow_run_name
from {{ source("ml_preproc", "two_tower_user_embedding_history") }} as main_table
where
    main_table.train_date = (
        select max(source_table.train_date) as last_train_date
        from
            {{ source("ml_preproc", "two_tower_user_embedding_history") }}
            as source_table
    )
