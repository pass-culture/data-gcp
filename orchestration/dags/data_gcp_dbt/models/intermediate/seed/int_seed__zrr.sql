SELECT 
    * except(geo_shape_insee, geo_shape_insee_1),
    ST_GEOGFROMTEXT(geo_shape_insee) AS geo_shape_insee
FROM {{ source('seed', 'zrr') }}