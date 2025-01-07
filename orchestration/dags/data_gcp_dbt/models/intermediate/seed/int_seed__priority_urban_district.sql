{{ config(**custom_table_config()) }}

select
    qpv.department_code,
    qpv.code_qpv,
    qpv.qpv_communes,
    qpv.qpv_name,
    qpv.department_name,
    qpv.insee_code,
    st_geogfromtext(qpv.geo_shape) as geo_shape,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).xmin as min_longitude,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).xmax as max_longitude,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).ymin as min_latitude,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).ymax as max_latitude
from {{ source("seed", "2024_insee_priority_urban_district") }} as qpv
