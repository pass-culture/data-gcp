-- TODO: Only export artist_id and artist_score if changes are detected
select artist_score_model.artist_id, artist_score_model.artist_score
from {{ ref("ml_metadata__artist_score") }} as artist_score_model
