{{ config(**custom_table_config(materialized="view")) }}

select distinct offer_product_id, artist_id, artist_type
from {{ source("ml_preproc", "product_artist_link") }}
