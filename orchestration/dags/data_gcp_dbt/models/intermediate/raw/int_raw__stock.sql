{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "stock_modified_date", "data_type": "timestamp"},
        )
    )
}}

select
    * except (stock_modified_date),
    cast(stock_modified_date as datetime) as stock_modified_date
from {{ ref("snapshot_raw__stock") }}
where {{ var("snapshot_filter") }}
