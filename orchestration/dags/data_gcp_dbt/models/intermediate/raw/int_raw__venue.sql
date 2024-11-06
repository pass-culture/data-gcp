{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "dbt_valid_to", "data_type": "timestamp"},
        )
    )
}}

select *
from {{ ref("snapshot_raw__venue") }}
where {{ var("snapshot_filter") }}
