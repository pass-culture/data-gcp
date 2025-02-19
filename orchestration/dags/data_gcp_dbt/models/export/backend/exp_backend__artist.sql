select
    artist_id,
    artist_name,
    artist_description,
    wikidata_image_file_url,
    wikidata_image_author,
    wikidata_image_license,
    wikidata_image_license_url,
from {{ ref("ml_linkage__artist") }}
