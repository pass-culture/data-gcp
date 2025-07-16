{{ config(**custom_table_config(materialized="view")) }}

select distinct
    artist_id,
    wiki_id as artist_wiki_id,
    offer_category_id,
    artist_type,
    artist_name as artist_offer_name,
    artist_name_to_match,
    action,
    comment
from {{ source("ml_preproc", "delta_artist_alias") }}
