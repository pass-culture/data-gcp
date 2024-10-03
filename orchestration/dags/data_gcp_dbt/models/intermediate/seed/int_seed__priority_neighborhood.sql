{{ config(**custom_table_config()) }}

select
    departement as department_code,
    code_quartier as code_qpv,
    commune_qp as qpv_communes,
    noms_des_communes_concernees as qpv_name,
    nom_reg as region_name,
    nom_dep as department_name,
    quartier_prioritaire as priority_neighborhood,
    code_insee as insee_code,
    st_geogfromtext(geoshape) as geo_shape,
    st_boundingbox(st_geogfromtext(geoshape)).xmin as min_longitude,
    st_boundingbox(st_geogfromtext(geoshape)).xmax as max_longitude,
    st_boundingbox(st_geogfromtext(geoshape)).ymin as min_latitude,
    st_boundingbox(st_geogfromtext(geoshape)).ymax as max_latitude
from {{ source("seed", "qpv") }}
