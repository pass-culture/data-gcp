{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "offer_date_updated", "data_type": "datetime"},
        )
    )
}}

select *
from {{ ref("snapshot__offer_backend") }}
where {{ var("snapshot_filter") }}
