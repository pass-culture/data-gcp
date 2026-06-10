{{ config(materialized="table") }}

with
    products as (
        select id as offer_product_id, ean
        from {{ ref("int_applicative__product") }}
        where ean is not null
    ),

    titelive_details as (
        select ean, title, artist, music_label, distributor
        from {{ ref("int_raw__product_titelive_details") }}
        where product_type = 'music'
    )

select
    prod.offer_product_id,
    titelive.ean,
    titelive.title,
    titelive.artist,
    titelive.music_label,
    titelive.distributor
from products as prod
inner join titelive_details as titelive on prod.ean = titelive.ean
