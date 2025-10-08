{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"},
        **custom_table_config(
            materialized="table",
            partition_by={"field": "offer_updated_date", "data_type": "datetime"},
        )
    )
}}

select
    * except (offer_updated_date),
    cast(offer_updated_date as datetime) as offer_updated_date
from {{ ref("snapshot_raw__offer") }}
qualify
    row_number() over (partition by offer_id order by dbt_valid_from desc) = 1
    and dbt_valid_to is not null
