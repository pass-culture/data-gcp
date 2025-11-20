{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "creation_timestamp",
                "data_type": "timestamp",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
            cluster_by=["user_id", "info_history_rank"],
        )
    )
}}

select
    user_id,
    info_history_rank,
    action_type,
    creation_timestamp,
    user_activity,
    user_previous_activity,
    has_confirmed,
    has_modified,
    has_modified_activity,
    has_modified_address,
    has_modified_city,
    has_modified_postal_code,
    epci_code,
    iris_internal_id
from {{ ref("int_history__user_beneficiary_information_history") }}
{% if is_incremental() %}
    where date(creation_timestamp) = date_sub(date("{{ ds() }}"), interval 1 day)
{% endif %}
