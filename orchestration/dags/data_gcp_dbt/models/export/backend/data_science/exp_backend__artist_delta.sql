-- depends_on: {{ ref('ml_linkage__future_artist') }}
-- depends_on: {{ ref('ml_linkage__future_product_artist_link') }}
select
    artist_id,
    artist_name,
    artist_description,
    artist_biography,
    artist_mediation_uuid,
    wikidata_id,
    wikipedia_url,
    wikidata_image_file_url,
    wikidata_image_author,
    wikidata_image_license,
    wikidata_image_license_url,
    spotify_id,
    isni_id,
    apple_music_id,
    deezer_id,
    genius_id,
    soundcloud_id,
    action,
    comment
from {{ ref("ml_linkage__delta_artist") }}
