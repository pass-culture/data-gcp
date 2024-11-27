{{ config(**custom_table_config(materialized="view")) }}

select distinct
    artist_id, offer_category_id, artist_type, artist_name as artist_offer_name
from {{ source("ml_preproc", "artist_linked") }}
where artist_id is not null
