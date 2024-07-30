SELECT
    SAFE_CAST(ending_datetime AS TIMESTAMP) AS ending_datetime,
    SAFE_CAST(beginning_datetime as TIMESTAMP) AS beginning_datetime,
    SAFE_CAST(is_geolocated AS bool) AS is_geolocated,
    SAFE_CAST(is_duo AS bool) AS is_duo,
    SAFE_CAST(is_event AS bool) AS is_event,
    SAFE_CAST(is_thing AS bool) AS is_thing,
    SAFE_CAST(price_max AS FLOAT64) AS price_max,
    SAFE_CAST(min_offers AS INT64) AS min_offers,
    *
    EXCEPT
    (
        ending_datetime,
        beginning_datetime,
        is_geolocated,
        is_duo,
        is_event,
        is_thing,
        price_max,
        min_offers
    )
FROM {{ source('raw', 'contentful_entry') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY execution_date DESC) = 1
