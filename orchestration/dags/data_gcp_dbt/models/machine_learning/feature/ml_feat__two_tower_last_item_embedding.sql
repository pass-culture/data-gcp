select
    main_table.item_id,
    main_table.item_embedding,
    main_table.train_date,
    main_table.mlflow_run_id,
    main_table.mlflow_experiment_name,
    main_table.mlflow_run_name
from {{ source("ml_preproc", "two_tower_item_embedding_history") }} as main_table
where
    main_table.train_date = (
        select max(source_table.train_date) as last_train_date
        from
            {{ source("ml_preproc", "two_tower_item_embedding_history") }}
            as source_table
    )
