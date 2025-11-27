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
    user_action_type,
    creation_timestamp,
    user_activity,
    user_previous_activity,
    user_age_at_info_creation,
    has_confirmed,
    has_modified,
    has_modified_activity,
    has_modified_address,
    has_modified_city,
    has_modified_postal_code,
    user_epci_code,
    user_iris_internal_id,
    user_region_name,
    user_department_name,
    user_density_label,
    user_density_macro_level,
    user_qpv_code,
    user_qpv_name
from {{ ref("int_history__user_beneficiary_information_history") }}
{% if is_incremental() %}
    where date(creation_timestamp) = date_sub(date("{{ ds() }}"), interval 1 day)
{% endif %}
