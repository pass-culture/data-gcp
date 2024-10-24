{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "offer_date_updated", "data_type": "datetime"},
        )
    )
}}

select *
from {{ ref("snapshot_raw__offer") }}
where {{ var("snapshot_filter") }}
