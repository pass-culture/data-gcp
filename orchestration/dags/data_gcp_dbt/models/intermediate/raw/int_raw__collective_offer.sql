{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={
                "field": "collective_offer_date_updated",
                "data_type": "datetime",
            },
        )
    )
}}

select *
from {{ ref("snapshot_raw__collective_offer") }}
where {{ var("snapshot_filter") }}
