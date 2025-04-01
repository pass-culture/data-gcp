{{ config(**custom_table_config()) }}


with
    institution_epci as (
        {{
            generate_seed_geolocation_query(
                source_table=["raw","applicative_database_educational_institution"],
                referential_table="int_seed__intercommunal_public_institution",
                id_column="institution_id",
                prefix_name="institution",
                columns=["epci_code", "epci_name"],
            )
        }}
    ),

    institution_qpv as (
        {{
            generate_seed_geolocation_query(
                source_table=["raw","applicative_database_educational_institution"],
                referential_table="int_seed__qpv",
                id_column="institution_id",
                prefix_name="institution",
                columns=["qpv_code", "qpv_name", "qpv_municipality"],
                geo_shape="qpv_geo_shape",
                geolocalisation_prefix="qpv_",
            )
        }}
    ),

    institution_zrr as (
        {{
            generate_seed_geolocation_query(
                source_table=["raw","applicative_database_educational_institution"],
                referential_table="int_seed__rural_revitalization_zone",
                id_column="institution_id",
                prefix_name="institution",
                columns=["zrr_level", "zrr_level_detail"],
            )
        }}
    ),

    institution_geo_iris as (
        {{
            generate_seed_geolocation_query(
                source_table=["raw","applicative_database_educational_institution"],
                referential_table="int_seed__geo_iris",
                id_column="institution_id",
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
                ],
                geo_shape="iris_shape",
            )
        }}
    )

select
    institution.educational_institution_id,
    institution.institution_id,
    institution.institution_city,
    institution.institution_postal_code,
    institution.institution_department_code,
    institution.institution_latitude,
    institution.institution_longitude,
    institution_geo_iris.iris_internal_id as institution_internal_iris_id,
    institution_geo_iris.density_label as institution_density_label,
    institution_geo_iris.density_macro_level as institution_macro_density_label,
    institution_geo_iris.density_level as institution_density_level,
    institution_geo_iris.academy_name as institution_academy_name,
    institution_geo_iris.region_name as institution_region_name,
    institution_geo_iris.department_name as institution_department_name,
    institution_epci.epci_name as institution_epci,
    institution_epci.epci_code,
    institution_qpv.qpv_code,
    institution_qpv.qpv_name,
    institution_qpv.qpv_municipality,
    institution_zrr.zrr_level,
    institution_zrr.zrr_level_detail,
    case
        when
            institution_qpv.qpv_code is null
            and institution.institution_latitude is null
            and institution.institution_longitude is null
        then null
        else institution_qpv.qpv_code is not null
    end as institution_in_qpv

from {{ source("raw", "applicative_database_educational_institution") }} as institution
left join
    institution_epci on institution.institution_id = institution_epci.institution_id
left join institution_qpv on institution.institution_id = institution_qpv.institution_id
left join institution_zrr on institution.institution_id = institution_zrr.institution_id
left join
    institution_geo_iris
    on institution.institution_id = institution_geo_iris.institution_id
