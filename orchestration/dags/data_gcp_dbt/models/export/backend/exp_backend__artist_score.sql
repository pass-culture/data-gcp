-- TODO: Only export artist_id and artist_score if changes are detected
select artist_id, artist_app_search_score, artist_pro_search_score
from {{ ref("ml_metadata__artist_score") }}
