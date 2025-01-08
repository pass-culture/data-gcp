{{ config(**custom_table_config()) }}

select
    qpv.department_code as qpv_department_code,
    qpv.department_name as qpv_department_name,
    qpv.qpv_code,
    qpv.qpv_municipality,
    qpv.qpv_name,
    qpv.insee_code,
    st_geogfromtext(qpv.geo_shape) as geo_shape,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).xmin as min_longitude,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).xmax as max_longitude,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).ymin as min_latitude,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).ymax as max_latitude
from {{ source("seed", "2024_insee_priority_urban_district") }} as qpv
