-- TODO: Remove this model once not imported anymore in Backend
select
    artist_id,
    artist_offer_name,
    null as artist_cluster_id,
    null as artist_wiki_id,
    offer_category_id,
    artist_type,
    action,
    comment
from {{ ref("ml_linkage__delta_artist_alias") }}
