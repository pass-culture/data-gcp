{{ config(**custom_table_config(materialized="view")) }}

with
    product_author as (
        select distinct
            offer_product_id,
            author as artist_name,
            offer_category_id,
            "author" as artist_type
        from {{ ref("mrt_global__offer") }}
        where offer_product_id != ""
    ),
    product_performer as (
        select distinct
            offer_product_id,
            performer as artist_name,
            offer_category_id,
            "performer" as artist_type
        from {{ ref("mrt_global__offer") }}
        where offer_product_id != ""
    ),
    product_artist as (
        select offer_product_id, artist_name, offer_category_id, artist_type
        from product_author
        union all
        select offer_product_id, artist_name, offer_category_id, artist_type
        from product_performer
    )
select distinct
    product_artist.offer_product_id,
    product_artist.artist_name,
    product_artist.offer_category_id,
    product_artist.artist_type
from product_artist
where product_artist.artist_name is not null
