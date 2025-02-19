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
    product_artists as (
        select offer_product_id, artist_name, offer_category_id, artist_type
        from product_author
        union all
        select offer_product_id, artist_name, offer_category_id, artist_type
        from product_performer
    ),
    artist_table as (
        select artist_name, offer_category_id, artist_type, artist_id
        from {{ source("ml_preproc", "artist_linked") }}
    )
select distinct
    product_artists.offer_product_id,
    artist_table.artist_id,
    product_artists.artist_type
from product_artists
left join
    artist_table
    on product_artists.artist_name = artist_table.artist_name
    and product_artists.offer_category_id = artist_table.offer_category_id
    and product_artists.artist_type = artist_table.artist_type
where artist_table.artist_id is not null
