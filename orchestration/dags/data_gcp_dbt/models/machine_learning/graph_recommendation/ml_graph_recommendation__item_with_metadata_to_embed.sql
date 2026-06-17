with
    offers as (
        select
            offer_id,
            offer_product_id,
            item_id,
            offer_name,
            offer_subcategory_id,
            case
                when offer_category_id = 'LIVRE'
                then 'book'
                when
                    offer_subcategory_id in (
                        'SUPPORT_PHYSIQUE_MUSIQUE_CD', 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
                    )
                then 'music'
            end as item_type
        from {{ ref("mrt_global__offer") }}
        where
            offer_category_id = 'LIVRE'
            or offer_subcategory_id
            in ('SUPPORT_PHYSIQUE_MUSIQUE_CD', 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE')
    ),

    -- One row per item: pick the offer with the best GTL coverage.
    -- Deduplication happens on the INNER JOIN result so the chosen offer
    -- is guaranteed to have metadata (items without any metadata are excluded).
    offers_with_best_metadata as (
        select
            offers.offer_id,
            offers.offer_product_id,
            offers.item_id,
            offers.item_type,
            offers.offer_name,
            offers.offer_subcategory_id,
            offer_metadata.titelive_gtl_id as raw_gtl_id,
            offer_metadata.gtl_label_level_1,
            offer_metadata.gtl_label_level_2,
            offer_metadata.gtl_label_level_3,
            offer_metadata.gtl_label_level_4,
            offer_metadata.author,
            -- Prefix gtl_id with item type initial (e.g. "b-" for books, "m-" for
            -- music).
            -- The same numeric GTL code has a different meaning depending on item type,
            -- so prefixing ensures they are treated as distinct nodes in the graph
            -- and produce separate embeddings. Must match _gtl_prefix() in
            -- heterograph_builder.py.
            -- If gtl_id is null or empty, we keep null (no prefix).
            case
                when
                    offer_metadata.titelive_gtl_id is not null
                    and offer_metadata.titelive_gtl_id != ''
                then left(offers.item_type, 1) || '-' || offer_metadata.titelive_gtl_id
            end as gtl_id
        from offers
        inner join
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

    book_titelive_metadata as (
        select offer_product_id, series_id, series, language_iso
        from {{ ref("int_global__product_titelive_paper_mapping") }}
    ),

    music_titelive_metadata as (
        select offer_product_id, music_label, distributor
        from {{ ref("int_global__product_titelive_music_mapping") }}
    )

select
    offers_with_best_metadata.item_type,
    offers_with_best_metadata.offer_id as example_offer_id,
    offers_with_best_metadata.offer_product_id,
    offers_with_best_metadata.item_id,
    offers_with_best_metadata.offer_name,
    offers_with_best_metadata.offer_subcategory_id,
    offers_with_best_metadata.gtl_id,
    offers_with_best_metadata.raw_gtl_id,
    offers_with_best_metadata.gtl_label_level_1,
    offers_with_best_metadata.gtl_label_level_2,
    offers_with_best_metadata.gtl_label_level_3,
    offers_with_best_metadata.gtl_label_level_4,
    offers_with_best_metadata.author,
    artist.artist_id,
    artist.artist_name,
    product_artist_link.artist_type,
    book_titelive_metadata.series_id,
    book_titelive_metadata.series,
    book_titelive_metadata.language_iso,
    music_titelive_metadata.music_label,
    music_titelive_metadata.distributor
from offers_with_best_metadata
left join product_artist_link using (offer_product_id)
left join artist using (artist_id)
left join book_titelive_metadata using (offer_product_id)
left join music_titelive_metadata using (offer_product_id)
