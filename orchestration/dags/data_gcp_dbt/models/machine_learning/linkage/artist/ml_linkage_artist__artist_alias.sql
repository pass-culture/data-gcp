select distinct
    artist_id, offer_category_id, artist_type, artist_name as artist_offer_name
from {{ source("ml_preproc", "linked_artists") }}
where artist_id is not null
