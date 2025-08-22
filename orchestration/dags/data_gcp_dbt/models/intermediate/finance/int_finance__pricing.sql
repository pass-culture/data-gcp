select
    id,
    status,
    bookingid,
    collective_booking_id,
    creationdate,
    valuedate,
    amount,
    standardrule,
    customruleid,
    revenue,
    pricing_point_id,
    venue_id,
    event_id
from {{ source("raw", "applicative_database_pricing") }}
