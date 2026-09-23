{{ config(**custom_table_config()) }}

with
    iris as (
        select
            iris_code,
            iris_label,
            iris_type,
            city_code,
            st_geogfromtext(geometry_wkt, make_valid => true) as iris_shape
        from {{ source("raw", "geo_iris") }}
    )

select
    iris.iris_code,
    iris.iris_label,
    iris.iris_type,
    iris.city_code,
    city.municipality_label as city_label,
    city.territorial_authority_code,
    city.district_code,
    city.sub_district_code,
    city.epci_code,
    city.epci_label,
    city.sub_district_label,
    city.district_label,
    city.department_code,
    region_department.dep_name as department_name,
    city.region_code,
    region_department.region_name,
    region_department.timezone,
    region_department.academy_name,
    city.territorial_authority_label,
    city.density_level,
    city.density_label,
    city.zrr_code,
    city.zrr_label,
    city.zrr_detail,
    city.frr_code,
    city.municipality_code,
    city.municipality_label,
    rural_city_type_data.geo_type as rural_city_type,
    iris.iris_shape,
    case
        when city.density_level >= 5
        then "rural"
        when city.density_level is not null
        then "urbain"
    end as density_macro_level,
    to_hex(md5(iris.iris_code)) as iris_internal_id,
    st_centroid(iris.iris_shape) as iris_centroid,
    st_boundingbox(iris.iris_shape).xmin as min_longitude,
    st_boundingbox(iris.iris_shape).xmax as max_longitude,
    st_boundingbox(iris.iris_shape).ymin as min_latitude,
    st_boundingbox(iris.iris_shape).ymax as max_latitude
from iris
left join
    {{ source("raw", "geo_municipality") }} as city on iris.city_code = city.city_code
left join
    {{ source("seed", "region_department") }} as region_department
    on city.department_code = region_department.num_dep
left join
    {{ source("seed", "rural_city_type_data") }} as rural_city_type_data
    on city.municipality_code = rural_city_type_data.geo_code
