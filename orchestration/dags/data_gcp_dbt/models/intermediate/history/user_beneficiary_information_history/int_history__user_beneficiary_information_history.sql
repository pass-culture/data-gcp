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


with
    user_epci as (
        {{
            generate_seed_geolocation_query(
                source_table="int_history__user_beneficiary_information_history_base",
                referential_table="int_seed__intercommunal_public_institution",
                id_column=["user_id", "info_history_rank"],
                prefix_name="user",
                columns=["epci_code"],
            )
        }}
    ),

    user_geo_iris as (
        {{
            generate_seed_geolocation_query(
                source_table="int_history__user_beneficiary_information_history_base",
                referential_table="int_seed__geo_iris",
                id_column=["user_id", "info_history_rank"],
                prefix_name="user",
                columns=[
                    "iris_internal_id",
                ],
                geo_shape="iris_shape",
            )
        }}
    )

select
    source_data.user_id,
    source_data.info_history_rank,
    source_data.action_type,
    source_data.creation_timestamp,
    source_data.user_activity,
    source_data.user_address,
    source_data.user_city,
    source_data.user_postal_code,
    source_data.user_previous_activity,
    source_data.user_previous_address,
    source_data.user_previous_city,
    source_data.user_previous_postal_code,
    source_data.has_confirmed,
    source_data.has_modified,
    source_data.has_modified_activity,
    source_data.has_modified_address,
    source_data.has_modified_city,
    source_data.has_modified_postal_code,
    source_data.user_longitude,
    source_data.user_latitude,
    user_epci.epci_code,
    user_geo_iris.iris_internal_id
from {{ ref("int_history__user_beneficiary_information_history_base") }} as source_data
left join user_epci using (user_id, info_history_rank)
left join user_geo_iris using (user_id, info_history_rank)
{% if is_incremental() %}
    where date(creation_timestamp) = date_sub(date("{{ ds() }}"), interval 1 day)
{% endif %}
