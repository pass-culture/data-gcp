{{ config(**custom_table_config(cluster_by="product_id")) }}

select
    product_artist_link_id,
    offer_product_id as product_id,
    artist_id,
    artist_type,
    creation_date as product_artist_link_creation_date,
    modification_date as product_artist_link_modification_date
from {{ ref("int_applicative__product_artist_link") }}
