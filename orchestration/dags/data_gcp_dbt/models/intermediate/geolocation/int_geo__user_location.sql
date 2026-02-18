{{ config(**custom_table_config()) }}


with
    user_epci as (
        {{
            generate_seed_geolocation_query(
                source_table="int_api_gouv__user_address",
                referential_table="int_seed__intercommunal_public_institution",
                id_column="user_id",
                prefix_name="user",
                columns=["epci_code", "epci_name"],
            )
        }}
    ),

    user_qpv as (
        {{
            generate_seed_geolocation_query(
                source_table="int_api_gouv__user_address",
                referential_table="int_seed__qpv",
                id_column="user_id",
                prefix_name="user",
                columns=["qpv_code", "qpv_name", "qpv_municipality"],
                geo_shape="qpv_geo_shape",
                geolocalisation_prefix="qpv_",
            )
        }}
    ),

    user_zrr as (
        {{
            generate_seed_geolocation_query(
                source_table="int_api_gouv__user_address",
                referential_table="int_seed__rural_revitalization_zone",
                id_column="user_id",
                prefix_name="user",
                columns=["zrr_level", "zrr_level_detail"],
            )
        }}
    ),

    user_geo_iris as (
        {{
            generate_seed_geolocation_query(
                source_table="int_api_gouv__user_address",
                referential_table="int_seed__geo_iris",
                id_column="user_id",
                prefix_name="user",
                columns=[
                    "iris_internal_id",
                    "region_name",
                    "city_label",
                    "city_code",
                    "rural_city_type",
                    "academy_name",
                    "department_name",
                    "density_label",
                    "density_macro_level",
                    "density_level",
                ],
                geo_shape="iris_shape",
            )
        }}
    )

select
    users.user_id,
    users.user_address_raw,
    users.user_postal_code,
    users.user_department_code,
    users.user_longitude,
    users.user_latitude,
    region_department.academy_name as user_academy_name,
    region_department.dep_name as user_department_name,
    region_department.region_name as user_region_name,
    user_geo_iris.iris_internal_id as user_iris_internal_id,
    user_geo_iris.city_label as user_city,
    cast(user_geo_iris.city_code as string) as user_city_code,
    user_geo_iris.rural_city_type as user_rural_city_type,
    user_geo_iris.density_label as user_density_label,
    user_geo_iris.density_macro_level as user_macro_density_label,
    user_geo_iris.density_level as user_density_level,
    user_epci.epci_name as user_epci,
    cast(user_epci.epci_code as string) as user_epci_code,
    user_qpv.qpv_code,
    user_qpv.qpv_name,
    user_qpv.qpv_municipality,
    user_zrr.zrr_level,
    user_zrr.zrr_level_detail,
    users.user_address_geocode_updated_at,
    users.geocode_type as user_address_geocode_type,
    case
        when
            user_qpv.qpv_code is null
            and users.user_latitude is null
            and users.user_longitude is null
        then null
        else user_qpv.qpv_code is not null
    end as user_is_in_qpv

from {{ ref("int_api_gouv__user_address") }} as users
left join user_epci on users.user_id = user_epci.user_id
left join user_qpv on users.user_id = user_qpv.user_id
left join user_zrr on users.user_id = user_zrr.user_id
left join user_geo_iris on users.user_id = user_geo_iris.user_id
-- ensure to have region and department name for non IRIS based regions (Wallis and
-- Futuna, New Caledonia, etc.)
left join
    {{ source("seed", "region_department") }} as region_department
    on users.user_department_code = region_department.num_dep
