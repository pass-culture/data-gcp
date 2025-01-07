select
    id as headline_offer_id,
    offerId as offer_id,
    venueId as venue_id,
    DATE(start_time) as headline_offer_start_date,
    DATE(end_time) as headline_offer_end_date
from {{ source("raw", "applicative_database_headline_offer") }}
