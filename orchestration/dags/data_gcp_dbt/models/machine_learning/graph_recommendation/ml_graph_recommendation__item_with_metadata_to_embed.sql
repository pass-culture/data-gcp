with
    product_artist_link as (
        select
            artist_id, artist_type, cast(offer_product_id as string) as offer_product_id
        from {{ source("raw", "applicative_database_product_artist_link") }}
    ),

    artist as (
        select artist_id, artist_name
        from {{ source("raw", "applicative_database_artist") }}
    ),

    offers as (
        select offer_id, offer_product_id, item_id, offer_name, offer_subcategory_id,
            case
                when offer_category_id = 'LIVRE'
                    then 'book'
                when offer_subcategory_id in (
                    'SUPPORT_PHYSIQUE_MUSIQUE_CD', 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
                )
                    then 'music'
            end as item_type
        from {{ ref("mrt_global__offer") }}
        where
            offer_category_id = 'LIVRE'
            or offer_subcategory_id in (
                'SUPPORT_PHYSIQUE_MUSIQUE_CD', 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
            )
    ),

    offers_with_best_metadata as (
        select
            offers.offer_id,
            offers.offer_product_id,
            offers.item_id,
            offers.item_type,
            offers.offer_name,
            offers.offer_subcategory_id,
            offer_metadata.titelive_gtl_id as gtl_id,
            offer_metadata.gtl_label_level_1,
            offer_metadata.gtl_label_level_2,
            -- levels 3 & 4 are only relevant for books; null for music
            case when offers.item_type = 'book'
                then offer_metadata.gtl_label_level_3
            end as gtl_label_level_3,
            case when offers.item_type = 'book'
                then offer_metadata.gtl_label_level_4
            end as gtl_label_level_4
        from offers
        left join
            {{ ref("mrt_global__offer_metadata") }} as offer_metadata using (offer_id)
        qualify
            row_number() over (
                partition by offers.item_id
                order by
                    (offer_metadata.gtl_label_level_1 is not null) desc,
                    offers.offer_id  -- deterministic tie breaker
            )
            = 1
    ),

    book_titelive_metadata as (
        select offer_product_id, series_id, series, language_iso
        from {{ ref("int_global__product_titelive_paper_mapping") }}
    ),

    music_titelive_metadata as (
        select offer_product_id, music_label, distributor
        from {{ ref("int_global__product_titelive_music_mapping") }}
    )

select
    o.item_type,
    o.offer_id as example_offer_id,
    o.offer_product_id,
    o.item_id,
    o.offer_name,
    o.offer_subcategory_id,
    o.gtl_id,
    o.gtl_label_level_1,
    o.gtl_label_level_2,
    o.gtl_label_level_3,
    o.gtl_label_level_4,
    artist.artist_id,
    artist.artist_name,
    product_artist_link.artist_type,
    book_titelive_metadata.series_id,
    book_titelive_metadata.series,
    book_titelive_metadata.language_iso,
    music_titelive_metadata.music_label,
    music_titelive_metadata.distributor
from offers_with_best_metadata as o
left join product_artist_link using (offer_product_id)
left join artist using (artist_id)
left join book_titelive_metadata using (offer_product_id)
left join music_titelive_metadata using (offer_product_id)

