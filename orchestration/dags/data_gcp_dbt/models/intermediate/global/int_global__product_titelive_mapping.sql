{{ config(materialized="table") }}

with
    products as (
        select id as offer_product_id, ean
        from {{ ref("int_applicative__product") }}
        where ean is not null
    ),

    titelive_details as (
        select ean, title, authors, contributor, language_iso, series, series_id
        from {{ ref("int_raw__product_titelive_details") }}
    )

select
    prod.offer_product_id,
    titelive.ean,
    titelive.title,
    titelive.authors,
    titelive.contributor,
    titelive.language_iso,
    titelive.series,
    titelive.series_id
from products as prod
inner join titelive_details as titelive on prod.ean = titelive.ean
