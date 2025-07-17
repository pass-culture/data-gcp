{{ config(**custom_table_config(materialized="view")) }}

select distinct offer_product_id, artist_id, artist_type, action, comment
from {{ source("ml_preproc", "delta_product_artist_link") }}
