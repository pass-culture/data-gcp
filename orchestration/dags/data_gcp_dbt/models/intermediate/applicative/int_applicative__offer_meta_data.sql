SELECT
    id,
    offer_id,
    videoUrl as offer_video_url
FROM {{ source("raw", "applicative_database_offer_meta_data") }}
