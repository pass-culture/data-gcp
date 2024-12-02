{{ config(**custom_table_config(materialized="view")) }}

select
    artist_id,
    artist_id_name as artist_name,
    wiki_id as artist_wiki_id,
    genre as artist_gender,
    description as artist_description,
    professions as artist_professions,
    image_file_url as wikidata_image_file_url,
    image_page_url as wikidata_image_page_url,
    image_author as wikidata_image_author,
    image_license as wikidata_image_license,
    image_license_url as wikidata_image_license_url,
from {{ source("ml_preproc", "artist_linked") }}
where is_cluster_representative = true
