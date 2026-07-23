-- depends_on: {{ ref('ml_linkage__future_artist') }}
-- depends_on: {{ ref('ml_linkage__future_product_artist_link') }}
select offer_product_id, artist_id, artist_type, action, comment
from {{ ref("ml_linkage__delta_product_artist_link") }}
