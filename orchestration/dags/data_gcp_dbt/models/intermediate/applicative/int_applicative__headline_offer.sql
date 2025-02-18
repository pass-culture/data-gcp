select
    id as headline_offer_id,
    offer_id,
    venue_id,
    safe.parse_timestamp(
        '%Y-%m-%d %H:%M:%E6S', regexp_extract(timespan, r'"([^"]+)"')
    ) as headline_beginning_time,,
    safe.parse_timestamp(
        '%Y-%m-%d %H:%M:%E6S', regexp_extract(timespan, r',"([^"]+)"')
    ) as headline_ending_time
from {{ source("raw", "applicative_database_headline_offer") }}
