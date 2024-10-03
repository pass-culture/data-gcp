{{ config(**custom_table_config()) }}

select
    codgeo as geo_code,  -- city_code
    libgeo as geo_name,
    zrr_simp as zrr_level,
    zrr_simp
    in ('C  Classée en ZRR', 'P  Commune partiellement classée en ZRR') as is_in_zrr,
    zonage_zrr as zrr_level_detail,
    code_postal as postal_code,
    code_insee as insee_code,
    insee_com as insee_commune,
    st_geogfromtext(geo_shape_insee) as geo_shape,
    st_boundingbox(st_geogfromtext(geo_shape_insee)).xmin as min_longitude,
    st_boundingbox(st_geogfromtext(geo_shape_insee)).xmax as max_longitude,
    st_boundingbox(st_geogfromtext(geo_shape_insee)).ymin as min_latitude,
    st_boundingbox(st_geogfromtext(geo_shape_insee)).ymax as max_latitude
from {{ source("seed", "zrr") }}
