{{ config(**custom_table_config(materialized="view")) }}

select distinct
    artist_id,
    wiki_id as artist_wiki_id,
    offer_category_id,
    artist_type,
    artist_name as artist_offer_name,
    artist_name_to_match
from {{ source("ml_preproc", "artist_alias") }}
where artist_id is not null
