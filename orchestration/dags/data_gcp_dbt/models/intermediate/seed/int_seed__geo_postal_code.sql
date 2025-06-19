{{ config(**custom_table_config()) }}

select
    postal_code,
    avg(safe_cast(centroid_latitude as float64)) as postal_approx_centroid_latitude,
    avg(safe_cast(centroid_longitude as float64)) as postal_approx_centroid_longitude
from {{ source("seed", "2025_insee_code") }}
group by postal_code
