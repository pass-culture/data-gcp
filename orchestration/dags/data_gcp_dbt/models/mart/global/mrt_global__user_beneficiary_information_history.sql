{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "user_information_created_at",
                "data_type": "timestamp",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
            cluster_by=["user_id", "user_information_rank"],
        )
    )
}}

select
    user_id,
    user_information_rank,
    user_information_action_type,
    user_information_created_at,
    user_activity,
    user_address,
    user_city,
    user_postal_code,
    user_previous_activity,
    user_previous_address,
    user_previous_city,
    user_previous_postal_code,
    user_has_confirmed_information,
    user_has_modified_information,
    user_has_modified_activity,
    user_has_modified_address,
    user_has_modified_city,
    user_has_modified_postal_code,
    user_longitude,
    user_latitude,
    user_epci_code,
    user_iris_internal_id,
    user_region_name,
    user_department_name,
    user_density_label,
    user_density_macro_level,
    user_qpv_code,
    user_qpv_name,
    user_age_at_info_creation
from {{ ref("int_history__user_beneficiary_information_history") }}
{% if is_incremental() %}
    where date(user_information_created_at) = date_sub(date("{{ ds() }}"), interval 1 day)
{% endif %}
