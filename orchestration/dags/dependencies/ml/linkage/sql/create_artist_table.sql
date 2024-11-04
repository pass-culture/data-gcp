select distinct
    artist_id as id,
    artist_id_name as name,
    genre,
    description,
    professions,
    img as img_url
from `{{ bigquery_tmp_dataset }}.linked_artists`
