{{ config(**custom_table_config()) }}

SELECT 
    postal_code,
    avg(safe_cast(centroid_latitude as float64)) as postal_approx_centroid_latitude,
    avg(safe_cast(centroid_longitude as float64)) as postal_approx_centroid_longitude
FROM {{ source("seed", "2025_insee_code") }}
GROUP BY postal_code
