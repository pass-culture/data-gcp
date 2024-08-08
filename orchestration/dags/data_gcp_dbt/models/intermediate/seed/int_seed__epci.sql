SELECT 
    * except(geo_shape),
    ST_GEOGFROMTEXT(geo_shape) AS geo_shape
FROM {{ source('seed', 'epci') }}