select artist_id, to_json_string(similar_artists_json) as similar_artists_json_string
from {{ ref("ml_reco__similar_artist") }}
