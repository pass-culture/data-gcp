{{
    config(
        **custom_table_config(
            materialized="table",
            partition_by={"field": "dbt_valid_to", "data_type": "timestamp"},
        )
    )
}}

select
    offerer_is_active,
    offerer_address,
    offerer_postal_code,
    offerer_city,
    offerer_id,
    offerer_creation_date,
    offerer_name,
    offerer_siren,
    offerer_validation_status,
    offerer_validation_date
from {{ ref("snapshot_raw__offerer") }}
where {{ var("snapshot_filter") }}
