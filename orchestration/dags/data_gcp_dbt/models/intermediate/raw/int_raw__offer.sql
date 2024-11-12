{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "offer_updated_date", "data_type": "datetime"},
        )
    )
}}

select *
from {{ ref("snapshot_raw__offer") }}
where {{ var("snapshot_filter") }}
