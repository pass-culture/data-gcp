select distinct
    artist_id, offer_category_id, artist_type, artist_name as artist_alias_name
from `{{ bigquery_tmp_dataset }}.linked_artists`
where artist_id is not null
