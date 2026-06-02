{{ config(**custom_table_config()) }}

with
    venue_epci as (
        {{
            generate_seed_geolocation_query(
                source_table="int_applicative__venue_address",
                referential_table="int_seed__intercommunal_public_institution",
                id_column="venue_id",
                prefix_name="venue",
                columns=["epci_code", "epci_name"],
            )
        }}
    ),

    venue_qpv as (
        {{
            generate_seed_geolocation_query(
                source_table="int_applicative__venue_address",
                referential_table="int_seed__qpv",
                id_column="venue_id",
                prefix_name="venue",
                columns=["qpv_code", "qpv_name", "qpv_municipality"],
                geo_shape="qpv_geo_shape",
                geolocalisation_prefix="qpv_",
            )
        }}
    ),

    venue_zrr as (
        {{
            generate_seed_geolocation_query(
                source_table="int_applicative__venue_address",
                referential_table="int_seed__rural_revitalization_zone",
                id_column="venue_id",
                prefix_name="venue",
                columns=["zrr_level", "zrr_level_detail", "is_in_zrr"],
            )
        }}
    ),

    venue_geo_iris as (
        {{
            generate_seed_geolocation_query(
                source_table="int_applicative__venue_address",
                referential_table="int_seed__geo_iris",
                id_column="venue_id",
                prefix_name="venue",
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
    venue.venue_id,
    venue.venue_postal_code,
    coalesce(venue.venue_department_code, "-1") as venue_department_code,
    venue.venue_latitude,
    venue.venue_longitude,
    venue.venue_street,
    coalesce(region_department.academy_name, "non localisé") as venue_academy_name,
    coalesce(region_department.dep_name, "non localisé") as venue_department_name,
    coalesce(region_department.region_name, "non localisé") as venue_region_name,
    coalesce(
        venue_geo_iris.iris_internal_id, "00000000000000000000000000000000"
    ) as venue_iris_internal_id,
    coalesce(venue_geo_iris.city_label, "non localisé") as venue_city,
    coalesce(cast(venue_geo_iris.city_code as string), "-1") as venue_city_code,
    coalesce(venue_geo_iris.rural_city_type, "non localisé") as venue_rural_city_type,
    coalesce(venue_geo_iris.density_label, "non localisé") as venue_density_label,
    coalesce(
        venue_geo_iris.density_macro_level, "non localisé"
    ) as venue_macro_density_label,
    coalesce(venue_geo_iris.density_level, -1) as venue_density_level,
    coalesce(venue_epci.epci_name, "non localisé") as venue_epci,
    coalesce(cast(venue_epci.epci_code as string), "-1") as venue_epci_code,
    venue_qpv.qpv_code,
    venue_qpv.qpv_name,
    venue_qpv.qpv_municipality,
    venue_zrr.zrr_level,
    venue_zrr.zrr_level_detail,
    venue_zrr.is_in_zrr as venue_in_zrr,
    case
        when
            venue_qpv.qpv_code is null
            and venue.venue_latitude is null
            and venue.venue_longitude is null
        then null
        else venue_qpv.qpv_code is not null
    end as venue_in_qpv

from {{ ref("int_applicative__venue_address") }} as venue
left join venue_epci on venue.venue_id = venue_epci.venue_id
left join venue_qpv on venue.venue_id = venue_qpv.venue_id
left join venue_zrr on venue.venue_id = venue_zrr.venue_id
left join venue_geo_iris on venue.venue_id = venue_geo_iris.venue_id
-- ensure to have region and department name for non IRIS based regions (Wallis and
-- Futuna, New Caledonia, etc.)
left join
    {{ source("seed", "region_department") }} as region_department
    on venue.venue_department_code = region_department.num_dep
