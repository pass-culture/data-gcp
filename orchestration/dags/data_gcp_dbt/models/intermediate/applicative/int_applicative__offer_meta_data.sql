select id, offer_id, videourl as offer_video_url
from {{ source("raw", "applicative_database_offer_meta_data") }}
