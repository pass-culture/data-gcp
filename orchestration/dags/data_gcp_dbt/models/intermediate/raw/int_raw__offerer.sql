{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "dbt_valid_to", "data_type": "timestamp"},
        )
    )
}}

select *
from {{ ref("snapshot_raw__offerer") }}
where {{ var("snapshot_filter") }}
