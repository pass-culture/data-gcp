{{ config(**custom_table_config(materialized="view")) }}

select
    artist_id,
    artist_name,
    artist_description,
    artist_biography,
    artist_mediation_uuid,
    wikidata_id,
    wikipedia_url,
    image_file_url as wikidata_image_file_url,
    image_author as wikidata_image_author,
    image_license as wikidata_image_license,
    image_license_url as wikidata_image_license_url,
    action,
    comment
from {{ source("ml_preproc", "delta_artist") }}
