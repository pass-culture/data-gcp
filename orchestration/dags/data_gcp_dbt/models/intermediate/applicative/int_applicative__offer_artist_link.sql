select
    offer_artist_link_id,
    artist_id,
    cast(offer_id as string) as offer_id,
    artist_type,
    artist_name
from {{ source("raw", "applicative_database_offer_artist_link") }}
