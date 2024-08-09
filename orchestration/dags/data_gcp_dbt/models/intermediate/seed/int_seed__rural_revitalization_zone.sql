{{ config(**custom_table_config()) }} 

SELECT 
    CODGEO AS geo_code, -- city_code
    LIBGEO AS geo_name,
    ZRR_SIMP AS zrr_level,
    ZRR_SIMP IN ('C  Classée en ZRR', 'P  Commune partiellement classée en ZRR') AS is_in_zrr,
    ZONAGE_ZRR AS zrr_level_detail,
    Code_Postal AS postal_code,
    Code_INSEE AS insee_code,
    insee_com AS insee_commune,
    ST_GEOGFROMTEXT(geo_shape_insee) AS geo_shape,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geo_shape_insee)).xmin AS min_longitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geo_shape_insee)).xmax AS max_longitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geo_shape_insee)).ymin AS min_latitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geo_shape_insee)).ymax AS max_latitude
FROM {{ source('seed', 'zrr') }}