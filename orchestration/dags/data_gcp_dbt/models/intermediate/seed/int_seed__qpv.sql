{{ config(**custom_table_config()) }}

select
    qpv.department_code as qpv_department_code,
    qpv.department_name as qpv_department_name,
    qpv.qpv_code,
    qpv.qpv_municipality,
    qpv.qpv_name,
    qpv.insee_code,
    st_geogfromtext(qpv.geo_shape) as qpv_geo_shape,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).xmin as qpv_min_longitude,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).xmax as qpv_max_longitude,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).ymin as qpv_min_latitude,
    st_boundingbox(st_geogfromtext(qpv.geo_shape)).ymax as qpv_max_latitude
from {{ source("seed", "2024_insee_qpv") }} as qpv
