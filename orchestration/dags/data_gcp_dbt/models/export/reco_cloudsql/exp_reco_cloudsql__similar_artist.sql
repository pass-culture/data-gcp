select artist_id, similar_artists_json from {{ ref("ml_reco__similar_artist") }}
