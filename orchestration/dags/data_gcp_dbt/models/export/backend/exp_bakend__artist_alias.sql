select distinct
    artist_id,
    artist_cluster_id,
    artist_wiki_id,
    offer_category_id,
    artist_type,
    artist_name as artist_offer_name
from {{ ref("ml_linkage__artist_alias") }}
where artist_id is not null
