select
    id,
    venue_id,
    pricing_point_id,
    pricing_point_link_beginning_date,
    pricing_point_link_ending_date
from {{ source("raw", "applicative_database_venue_pricing_point_link") }}
