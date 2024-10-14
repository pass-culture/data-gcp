{{ config(**custom_table_config()) }}

select
    iris_code,
    iris_label,
    city_code,
    city_label,
    territorial_authority_code,
    district_code,
    sub_district_code,
    epci_code,
    epci_label,
    sub_district_label,
    district_label,
    department_code,
    department_name,
    region_code,
    region_name,
    timezone,
    academy_name,
    territorial_authority_label,
    has_qpv,
    density_level,
    density_label,
    density_macro_level,
    geo_code,
    rural_city_type_data.geo_type as rural_city_type,
    iris_internal_id,
    st_geogpoint(
        cast(split(replace(iris_centroid, 'POINT(', ''), ' ')[ordinal(1)] as float64),
        cast(
            split(replace(replace(iris_centroid, 'POINT(', ''), ')', ''), ' ')[
                ordinal(2)
            ] as float64
        )
    ) as iris_centroid,
    st_geogfromtext(iris_shape) as iris_shape,
    st_boundingbox(st_geogfromtext(iris_shape)).xmin min_longitude,
    st_boundingbox(st_geogfromtext(iris_shape)).xmax as max_longitude,
    st_boundingbox(st_geogfromtext(iris_shape)).ymin as min_latitude,
    st_boundingbox(st_geogfromtext(iris_shape)).ymax as max_latitude
from {{ source("seed", "geo_iris") }} gi
left join {{ source("seed", "rural_city_type_data") }} using (geo_code)
