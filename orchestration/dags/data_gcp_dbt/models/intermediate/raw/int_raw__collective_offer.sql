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

select
    * except (collective_offer_date_updated),
    cast(collective_offer_date_updated as datetime) as collective_offer_date_updated
from {{ ref("snapshot_raw__collective_offer") }}
where {{ var("snapshot_filter") }}
