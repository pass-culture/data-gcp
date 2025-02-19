select offer_product_id, artist_id, artist_type
from {{ ref("ml_linkage__product_artist_link") }}
