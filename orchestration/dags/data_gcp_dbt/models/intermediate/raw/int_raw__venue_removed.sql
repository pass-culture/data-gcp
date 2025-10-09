{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"},
        **custom_table_config(
            materialized="table"
        )
    )
}}

select
    *
from {{ ref("snapshot_raw__venue") }}
qualify
    row_number() over (partition by venue_id order by dbt_valid_from desc) = 1
    and dbt_valid_to is not null
