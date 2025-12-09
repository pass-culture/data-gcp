{{
    config(
        materialized="table",
        partition_by={
            "field": "user_information_created_at",
            "data_type": "timestamp",
            "granularity": "day",
        },
        cluster_by=["user_id", "user_information_rank"],
    )
}}

with
    -- CTE: Enrich with EPCI (intercommunal institution) information
    user_epci as (
        {{
            generate_seed_geolocation_query(
                source_table="int_history__user_beneficiary_information_history_base",
                referential_table="int_seed__intercommunal_public_institution",
                id_column=["user_id", "user_information_rank"],
                prefix_name="user",
                columns=["epci_code"],
            )
        }}
    ),

    -- CTE: Enrich with IRIS geographical data including density and region
    user_geo_iris as (
        {{
            generate_seed_geolocation_query(
                source_table="int_history__user_beneficiary_information_history_base",
                referential_table="int_seed__geo_iris",
                id_column=["user_id", "user_information_rank"],
                prefix_name="user",
                columns=[
                    "iris_internal_id",
                    "region_name",
                    "department_name",
                    "density_label",
                    "density_macro_level",
                ],
                geo_shape="iris_shape",
            )
        }}
    ),

    -- CTE: Enrich with QPV (Quartier Prioritaire de la Ville) data
    user_qpv as (
        {{
            generate_seed_geolocation_query(
                source_table="int_history__user_beneficiary_information_history_base",
                referential_table="int_seed__qpv",
                id_column=["user_id", "user_information_rank"],
                prefix_name="user",
                columns=["qpv_code", "qpv_name"],
                geo_shape="qpv_geo_shape",
                geolocalisation_prefix="qpv_",
            )
        }}
    )

select
    source_data.user_id,
    source_data.user_information_rank,
    source_data.user_information_action_type,
    source_data.user_information_created_at,
    source_data.user_activity,
    source_data.user_address,
    source_data.user_city,
    source_data.user_postal_code,
    source_data.user_previous_activity,
    source_data.user_previous_address,
    source_data.user_previous_city,
    source_data.user_previous_postal_code,
    source_data.user_has_confirmed_information,
    source_data.user_has_modified_information,
    source_data.user_has_modified_activity,
    source_data.user_has_modified_address,
    source_data.user_has_modified_city,
    source_data.user_has_modified_postal_code,
    source_data.user_longitude,
    source_data.user_latitude,
    -- EPCI data
    user_epci.epci_code as user_epci_code,
    -- IRIS geographical data
    user_geo_iris.iris_internal_id as user_iris_internal_id,
    user_geo_iris.region_name as user_region_name,
    user_geo_iris.department_name as user_department_name,
    user_geo_iris.density_label as user_density_label,
    user_geo_iris.density_macro_level as user_density_macro_level,
    -- QPV data
    user_qpv.qpv_code as user_qpv_code,
    user_qpv.qpv_name as user_qpv_name,
    -- User age at information creation
    source_data.user_age_at_information_creation
from {{ ref("int_history__user_beneficiary_information_history_base") }} as source_data
left join
    user_epci
    on source_data.user_id = user_epci.user_id
    and source_data.user_information_rank = user_epci.user_information_rank
left join
    user_geo_iris
    on source_data.user_id = user_geo_iris.user_id
    and source_data.user_information_rank = user_geo_iris.user_information_rank
left join
    user_qpv
    on source_data.user_id = user_qpv.user_id
    and source_data.user_information_rank = user_qpv.user_information_rank
