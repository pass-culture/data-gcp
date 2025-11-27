select
    product_artist_link_id,
    artist_id,
    cast(offer_product_id as string) as offer_product_id,
    artist_type,
    date(date_created) as creation_date,
    date(date_modified) as modification_date
from {{ source("raw", "applicative_database_product_artist_link") }}
