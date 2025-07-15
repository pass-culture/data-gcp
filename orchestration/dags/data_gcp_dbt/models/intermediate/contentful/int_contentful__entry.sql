select
    safe_cast(ending_datetime as timestamp) as ending_datetime,
    safe_cast(beginning_datetime as timestamp) as beginning_datetime,
    safe_cast(is_geolocated as bool) as is_geolocated,
    safe_cast(is_duo as bool) as is_duo,
    safe_cast(is_event as bool) as is_event,
    safe_cast(is_thing as bool) as is_thing,
    safe_cast(price_max as float64) as price_max,
    safe_cast(min_offers as int64) as min_offers,
    {{ clean_str("title") }} as title,
    {{ clean_str("offer_title") }} as offer_title,
    regexp_extract(url, r'to/([^/?#]+)') as typeform_id,
    * except (
        ending_datetime,
        beginning_datetime,
        is_geolocated,
        is_duo,
        is_event,
        is_thing,
        price_max,
        min_offers,
        title,
        offer_title
    )
from {{ source("raw", "contentful_entry") }}
qualify row_number() over (partition by id order by execution_date desc) = 1
