{{ config(**custom_table_config(materialized="view")) }}

with
    product_artist as (
        select offer_product_id, artist_name, offer_category_id, artist_type
        from {{ ref("ml_linkage__product_to_link") }}
    ),
    artist_table as (
        select artist_name, offer_category_id, artist_type, artist_id
        from {{ source("ml_preproc", "artist_linked") }}
    )

select distinct
    product_artist.offer_product_id, artist_table.artist_id, product_artist.artist_type
from product_artist
left join
    artist_table
    on product_artist.artist_name = artist_table.artist_name
    and product_artist.offer_category_id = artist_table.offer_category_id
    and product_artist.artist_type = artist_table.artist_type
where artist_table.artist_id is not null
