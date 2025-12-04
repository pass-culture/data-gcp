select
    artist_id,
    artist_name,
    artist_description,
    wikidata_image_file_url,
    wikidata_image_license,
    wikidata_image_license_url,
    wikidata_image_author,
    date(date_created) as creation_date,
    date(date_modified) as modification_date
from {{ source("raw", "applicative_database_artist") }}
