select offer_product_id, artist_id, artist_type, action, comment
from {{ ref("ml_linkage__delta_product_artist_link") }}
