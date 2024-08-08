SELECT 
    * except(geoshape),
    ST_GEOGFROMTEXT(geoshape) AS geoshape
FROM {{ source('seed', 'qpv') }}