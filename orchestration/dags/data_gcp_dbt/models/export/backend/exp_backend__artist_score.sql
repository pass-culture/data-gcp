select artist_id, artist_score
from {{ ref("ml_metadata__artist_score") }}
where artist_score >= 3.0
