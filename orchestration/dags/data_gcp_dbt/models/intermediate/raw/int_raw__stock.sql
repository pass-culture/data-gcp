{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "stock_modified_date", "data_type": "timestamp"},
        )
    )
}}

select *
from {{ ref("snapshot_raw__stock") }}
where {{ var("snapshot_filter") }}
