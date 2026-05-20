{{
    config(
        materialized="table",
        tags=["ml", "graph_recommendation", "test"],
    )
}}

-- Test dataset to verify that book and music items sharing the same GTL code
-- end up in DIFFERENT clusters/spaces in the embedding graph.
--
-- Strategy:
-- 1. Identify GTL IDs that exist for both books and music (level-2 GTL, i.e.
-- ending in '0000', excluding the catch-all '00000000').
-- 2. For each such shared GTL, sample up to 500 books and 500 music items
-- → target total ~10 000 rows (capped to exactly 10 000 via final QUALIFY).
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
    -- Step 1 : find GTL IDs that appear for both books and music,
    -- then keep at most 2 GTL codes per level-1 group (first 2 digits)
    -- to ensure diversity across music/book genres.
    -- -------------------------------------------------------------------------
    gtl_coverage as (
        select titelive_gtl_id
        from {{ ref("mrt_global__offer_metadata") }}
        where
            offer_category_id in ('LIVRE', 'MUSIQUE_ENREGISTREE')
            and titelive_gtl_id != '00000000'
            and titelive_gtl_id like '%0000'  -- level-2 GTL codes only
        group by titelive_gtl_id
        having
            count(case when gtl_type = 'BOOK' then 1 end) > 0
            and count(case when gtl_type = 'MUSIC' then 1 end) > 0
    ),

    -- Limit to 2 GTL codes per level-1 group to spread across genres
    gtl_coverage_diverse as (
        select titelive_gtl_id as gtl_id
        from gtl_coverage
        qualify
            row_number() over (
                -- group by first 2 digits (level-1 prefix)
                partition by substr(titelive_gtl_id, 1, 2) order by titelive_gtl_id
            )
            <= 2
    ),

    -- -------------------------------------------------------------------------
    -- Step 2 : pull offers for the matching GTL IDs
    -- -------------------------------------------------------------------------
    offers as (
        select
            o.offer_id,
            o.offer_product_id,
            o.item_id,
            o.offer_name,
            o.offer_subcategory_id,
            case
                when o.offer_category_id = 'LIVRE'
                then 'book'
                when
                    o.offer_subcategory_id in (
                        'SUPPORT_PHYSIQUE_MUSIQUE_CD', 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
                    )
                then 'music'
            end as item_type
        from {{ ref("mrt_global__offer") }} as o
        where
            o.offer_category_id = 'LIVRE'
            or o.offer_subcategory_id
            in ('SUPPORT_PHYSIQUE_MUSIQUE_CD', 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE')
    ),

    -- Keep one offer per item, picking the one with the best GTL coverage,
    -- then restrict to items whose best GTL is one of the selected shared GTLs.
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
            case
                when offers.item_type = 'book' then offer_metadata.gtl_label_level_3
            end as gtl_label_level_3,
            case
                when offers.item_type = 'book' then offer_metadata.gtl_label_level_4
            end as gtl_label_level_4
        from offers
        inner join
            {{ ref("mrt_global__offer_metadata") }} as offer_metadata using (offer_id)
        qualify
            -- deduplicate to one row per item BEFORE filtering on gtl_coverage,
            -- so that the 500-cap in step 3 counts truly distinct items.
            row_number() over (
                partition by offers.item_id
                order by
                    (offer_metadata.gtl_label_level_1 is not null) desc, offers.offer_id
            )
            = 1
    ),

    -- Keep only items whose best GTL falls in the diverse shared set
    offers_filtered as (
        select owbm.*
        from offers_with_best_metadata as owbm
        inner join gtl_coverage_diverse using (gtl_id)
    ),

    -- -------------------------------------------------------------------------
    -- Step 3 : sample up to 500 items per (gtl_id, item_type) pair,
    -- then cap the total to 10 000 rows (5 000 books + 5 000 music)
    -- -------------------------------------------------------------------------
    sampled as (
        select *
        from offers_filtered
        qualify
            row_number() over (
                partition by gtl_id, item_type order by item_id  -- deterministic ordering
            )
            <= 500  -- at most 500 distinct items per (gtl, type) cell
    ),

    capped as (
        select *
        from sampled
        qualify
            row_number() over (
                partition by item_type order by gtl_id, item_id  -- deterministic ordering
            )
            <= 5000
    ),

    -- -------------------------------------------------------------------------
    -- Step 4 : join titelive metadata
    -- -------------------------------------------------------------------------
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
from capped as o
left join product_artist_link using (offer_product_id)
left join artist using (artist_id)
left join book_titelive_metadata using (offer_product_id)
left join music_titelive_metadata using (offer_product_id)
