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

    -- -------------------------------------------------------------------------
    -- BOOKS
    -- -------------------------------------------------------------------------
    book_offers as (
        select offer_id, offer_product_id, item_id, offer_name, offer_subcategory_id
        from {{ ref("mrt_global__offer") }}
        where offer_category_id = "LIVRE"
    ),

    book_offers_with_best_metadata as (
        select
            book_offers.offer_id,
            book_offers.offer_product_id,
            book_offers.item_id,
            book_offers.offer_name,
            book_offers.offer_subcategory_id,
            offer_metadata.titelive_gtl_id as gtl_id,
            offer_metadata.gtl_label_level_1,
            offer_metadata.gtl_label_level_2,
            offer_metadata.gtl_label_level_3,
            offer_metadata.gtl_label_level_4
        from book_offers
        left join
            {{ ref("mrt_global__offer_metadata") }} as offer_metadata using (offer_id)
        qualify
            row_number() over (
                partition by book_offers.item_id
                order by
                    (offer_metadata.gtl_label_level_1 is not null) desc,
                    book_offers.offer_id  -- deterministic tie breaker
            )
            = 1
    ),

    book_titelive_metadata as (
        select offer_product_id, series_id, series, language_iso
        from {{ ref("int_global__product_titelive_paper_mapping") }}
    ),

    books as (
        select
            'book' as item_type,
            b.offer_id as example_offer_id,
            b.offer_product_id,
            b.item_id,
            b.offer_name,
            b.offer_subcategory_id,
            b.gtl_id,
            b.gtl_label_level_1,
            b.gtl_label_level_2,
            b.gtl_label_level_3,
            b.gtl_label_level_4,
            artist.artist_id,
            artist.artist_name,
            product_artist_link.artist_type,
            book_titelive_metadata.series_id,
            book_titelive_metadata.series,
            book_titelive_metadata.language_iso,
            -- music-specific fields (null for books)
            null as music_label,
            null as distributor
        from book_offers_with_best_metadata as b
        left join product_artist_link using (offer_product_id)
        left join artist using (artist_id)
        left join book_titelive_metadata using (offer_product_id)
    ),

    -- -------------------------------------------------------------------------
    -- MUSIC (CD / Vinyle)
    -- -------------------------------------------------------------------------
    music_offers as (
        select offer_id, offer_product_id, item_id, offer_name, offer_subcategory_id
        from {{ ref("mrt_global__offer") }}
        where
            offer_category_id = "MUSIQUE_ENREGISTREE"
            and offer_subcategory_id
            in ('SUPPORT_PHYSIQUE_MUSIQUE_CD', 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE')
    ),

    music_offers_with_best_metadata as (
        select
            music_offers.offer_id,
            music_offers.offer_product_id,
            music_offers.item_id,
            music_offers.offer_name,
            music_offers.offer_subcategory_id,
            offer_metadata.titelive_gtl_id as gtl_id,
            offer_metadata.gtl_label_level_1,
            offer_metadata.gtl_label_level_2
        from music_offers
        left join
            {{ ref("mrt_global__offer_metadata") }} as offer_metadata using (offer_id)
        qualify
            row_number() over (
                partition by music_offers.item_id
                order by
                    (offer_metadata.gtl_label_level_1 is not null) desc,
                    music_offers.offer_id  -- deterministic tie breaker
            )
            = 1
    ),

    music_titelive_metadata as (
        select offer_product_id, music_label, distributor
        from {{ ref("int_global__product_titelive_music_mapping") }}
    ),

    music as (
        select
            'music' as item_type,
            m.offer_id as example_offer_id,
            m.offer_product_id,
            m.item_id,
            m.offer_name,
            m.offer_subcategory_id,
            m.gtl_id,
            m.gtl_label_level_1,
            m.gtl_label_level_2,
            -- music has no GTL levels 3 & 4
            null as gtl_label_level_3,
            null as gtl_label_level_4,
            artist.artist_id,
            artist.artist_name,
            product_artist_link.artist_type,
            -- book-specific fields (null for music)
            null as series_id,
            null as series,
            null as language_iso,
            music_titelive_metadata.music_label,
            music_titelive_metadata.distributor
        from music_offers_with_best_metadata as m
        left join product_artist_link using (offer_product_id)
        left join artist using (artist_id)
        left join music_titelive_metadata using (offer_product_id)
    )

select *
from books

union all

select *
from music
