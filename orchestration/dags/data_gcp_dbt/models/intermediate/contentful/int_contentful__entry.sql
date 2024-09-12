select
    SAFE_CAST(ending_datetime as TIMESTAMP) as ending_datetime,
    SAFE_CAST(beginning_datetime as TIMESTAMP) as beginning_datetime,
    SAFE_CAST(is_geolocated as BOOL) as is_geolocated,
    SAFE_CAST(is_duo as BOOL) as is_duo,
    SAFE_CAST(is_event as BOOL) as is_event,
    SAFE_CAST(is_thing as BOOL) as is_thing,
    SAFE_CAST(price_max as FLOAT64) as price_max,
    SAFE_CAST(min_offers as INT64) as min_offers,
    *
    except
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
from {{ source('raw', 'contentful_entry') }}
qualify ROW_NUMBER() over (partition by id order by execution_date desc) = 1
