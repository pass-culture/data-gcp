select
    id as headline_offer_id,
    offerid as offer_id,
    venueid as venue_id,
    date(start_time) as headline_offer_start_date,
    date(end_time) as headline_offer_end_date
from {{ source("raw", "applicative_database_headline_offer") }}
