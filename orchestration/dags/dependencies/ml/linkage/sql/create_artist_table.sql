select distinct
    artist_id as id,
    artist_id_name as name,
    genre,
    description,
    professions,
    image_file_url,
    image_page_url,
    image_author,
    image_license,
    image_license_url,
from `{{ bigquery_tmp_dataset }}.linked_artists`
