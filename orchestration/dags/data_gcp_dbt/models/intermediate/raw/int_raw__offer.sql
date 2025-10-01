{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "offer_updated_date", "data_type": "datetime"},
        )
    )
}}

select
    * except (offer_updated_date, offer_is_active),
    cast(offer_updated_date as datetime) as offer_updated_date,
    case
        when dbt_valid_to is null
        then offer_is_active
        else false
    end as offer_is_active
from {{ ref("snapshot_raw__offer") }}
qualify row_number() over (partition by offer_id order by dbt_valid_from desc) = 1
