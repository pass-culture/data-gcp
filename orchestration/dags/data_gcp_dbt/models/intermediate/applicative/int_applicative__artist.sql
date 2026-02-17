select
    artist_id,
    artist_name,
    artist_description,
    artist_biography,
    artist_mediation_uuid,
    wikidata_id,
    wikipedia_url,
    wikidata_image_file_url,
    wikidata_image_license,
    wikidata_image_license_url,
    wikidata_image_author,
    artist_app_search_score,
    artist_pro_search_score,
    date(date_created) as creation_date,
    date(date_modified) as modification_date
from {{ source("raw", "applicative_database_artist") }}
