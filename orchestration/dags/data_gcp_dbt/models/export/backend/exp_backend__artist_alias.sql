select
    artist_id,
    artist_offer_name,
    null as artist_cluster_id,
    artist_wiki_id,
    offer_category_id,
    artist_type
from {{ ref("ml_linkage__artist_alias") }}
