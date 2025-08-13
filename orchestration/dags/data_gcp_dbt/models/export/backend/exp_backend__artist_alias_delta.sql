select
    artist_id,
    artist_offer_name,
    null as artist_cluster_id,
    artist_wiki_id,
    offer_category_id,
    artist_type,
    action,
    comment
from {{ ref("ml_linkage__delta_artist_alias") }}
