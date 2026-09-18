{{ config(**custom_table_config()) }}


with
    institution_qpv as (
        {{
            generate_seed_geolocation_query(
                source_table="int_api_gouv__educational_institution_address",
                referential_table="int_seed__qpv",
                id_column="educational_institution_id",
                prefix_name="institution",
                columns=["qpv_code", "qpv_name", "qpv_municipality"],
                geo_shape="qpv_geo_shape",
                geolocalisation_prefix="qpv_",
            )
        }}
    ),

    institution_geo_iris as (
        {{
            generate_seed_geolocation_query(
                source_table="int_api_gouv__educational_institution_address",
                referential_table="int_seed__geo_iris",
                id_column="educational_institution_id",
                prefix_name="institution",
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
                    "epci_code",
                    "epci_label",
                ],
                geo_shape="iris_shape",
            )
        }}
    )

select
    institution.educational_institution_id,
    institution.institution_postal_code,
    institution.institution_department_code,
    region_department.academy_name as institution_academy_name,
    region_department.dep_name as institution_department_name,
    region_department.region_name as institution_region_name,
    institution.institution_latitude,
    institution.institution_longitude,
    institution_geo_iris.iris_internal_id as institution_internal_iris_id,
    institution_geo_iris.density_label as institution_density_label,
    institution_geo_iris.density_macro_level as institution_macro_density_label,
    institution_geo_iris.density_level as institution_density_level,
    institution_geo_iris.city_label as institution_city,
    cast(institution_geo_iris.city_code as string) as institution_city_code,
    institution_geo_iris.epci_label as institution_epci,
    institution_geo_iris.epci_code as institution_epci_code,
    institution_qpv.qpv_code,
    institution_qpv.qpv_name,
    institution_qpv.qpv_municipality,
    institution.geocode_type as institution_address_geocode_type,
    case
        when
            institution_qpv.qpv_code is null
            and institution.institution_latitude is null
            and institution.institution_longitude is null
        then null
        else institution_qpv.qpv_code is not null
    end as institution_in_qpv

from {{ ref("int_api_gouv__educational_institution_address") }} as institution
left join
    institution_qpv
    on institution.educational_institution_id
    = institution_qpv.educational_institution_id
left join
    institution_geo_iris
    on institution.educational_institution_id
    = institution_geo_iris.educational_institution_id
-- ensure to have region and department name for non IRIS based regions (Wallis and
-- Futuna, New Caledonia, etc.)
left join
    {{ source("seed", "region_department") }} as region_department
    on institution.institution_department_code = region_department.num_dep
