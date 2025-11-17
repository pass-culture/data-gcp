with
    offers as (
        select offer_id, offer_product_id, item_id, offer_name, offer_subcategory_id
        from {{ ref("mrt_global__offer") }}
        where offer_category_id = "LIVRE"
    ),

    offers_with_best_metadata as (
        select
            offers.offer_id,
            offers.offer_product_id,
            offers.item_id,
            offers.offer_name,
            offers.offer_subcategory_id,
            offer_metadata.titelive_gtl_id as gtl_id,
            offer_metadata.gtl_type,
            offer_metadata.gtl_label_level_1,
            offer_metadata.gtl_label_level_2,
            offer_metadata.gtl_label_level_3,
            offer_metadata.gtl_label_level_4,
            offer_metadata.author
        from offers
        left join
            {{ ref("mrt_global__offer_metadata") }} as offer_metadata using (offer_id)
        qualify
            row_number() over (
                partition by offers.item_id
                order by
                    (offer_metadata.gtl_label_level_1 is not null) desc, offers.offer_id  -- deterministic tie breaker
            )
            = 1
    ),

    product_artist_link as (
        select
            artist_id, artist_type, cast(offer_product_id as string) as offer_product_id
        from {{ source("raw", "applicative_database_product_artist_link") }}
    ),

    artist as (
        select artist_id, artist_name
        from {{ source("raw", "applicative_database_artist") }}
    ),

    titelive_metadata as (
        select offer_product_id, series_id, series, language_iso
        from {{ ref("int_global__product_titelive_mapping") }}
    )

select
    offers_with_best_metadata.offer_id as example_offer_id,
    offers_with_best_metadata.offer_product_id,
    offers_with_best_metadata.item_id,
    offers_with_best_metadata.offer_name,
    offers_with_best_metadata.offer_subcategory_id,
    offers_with_best_metadata.gtl_id,
    offers_with_best_metadata.gtl_type,
    offers_with_best_metadata.gtl_label_level_1,
    offers_with_best_metadata.gtl_label_level_2,
    offers_with_best_metadata.gtl_label_level_3,
    offers_with_best_metadata.gtl_label_level_4,
    offers_with_best_metadata.author,
    artist.artist_id,
    artist.artist_name,
    product_artist_link.artist_type,
    titelive_metadata.series_id,
    titelive_metadata.series,
    titelive_metadata.language_iso
from offers_with_best_metadata
left join product_artist_link using (offer_product_id)
left join artist using (artist_id)
left join titelive_metadata using (offer_product_id)
