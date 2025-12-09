select
    artist_id,
    artist_name,
    artist_description,
    artist_biography,
    wikidata_id,
    wikipedia_url,
    wikidata_image_file_url,
    wikidata_image_author,
    wikidata_image_license,
    wikidata_image_license_url,
    action,
    comment
from {{ ref("ml_linkage__delta_artist") }}
