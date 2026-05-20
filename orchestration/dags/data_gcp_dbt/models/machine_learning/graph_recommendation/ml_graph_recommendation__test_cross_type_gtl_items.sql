{{
    config(
        materialized="table",
        tags=["ml", "graph_recommendation", "test"],
    )
}}

-- Test dataset to verify that book and music items sharing the same GTL code
-- end up in DIFFERENT clusters/spaces in the embedding graph.
--
-- 20 GTL codes selected manually — each exists for BOTH books and music with
-- high volume (> 50k products each).  They cover diverse level-1 genres to
-- avoid clustering artefacts from a single genre dominating the dataset.
--
-- Target: 20 GTL × 500 books + 20 GTL × 500 music = 20 000 rows
--
-- Selected GTL codes (titelive_gtl_id | BOOK label | MUSIC label):
-- 01020000 | Littérature - Romans & Nouvelles de genre     | Musique Classique -
-- Musique de chambre
-- 01030000 | Littérature - Œuvres classiques               | Musique Classique -
-- Liturgie
-- 01050000 | Littérature - Récit                           | Musique Classique -
-- Musique baroque
-- 01060000 | Littérature - Biographie / Témoignage litt.   | Musique Classique -
-- Classique Symphonie
-- 01080000 | Littérature - Théâtre                         | Musique Classique -
-- Classique Sonate
-- 01090000 | Littérature - Poésie                          | Musique Classique -
-- Opéra / Chant lyrique
-- 02040000 | Jeunesse - Littérature Enfants                | Jazz / Blues - Jazz Rock
-- / Fusion / Funk
-- 02050000 | Jeunesse - Littérature Jeunes Adultes         | Jazz / Blues - Blues
-- 03020000 | BD / Comics / Mangas - Bandes dessinées       | BO - Musique de Séries TV
-- 04030000 | Vie pratique - Arts de la table / Gastro.     | Electro - House Music
-- 06010000 | Arts et spectacles - Généralités sur l'art    | Rock - Rock international
-- 06030000 | Arts et spectacles - Musique                  | Rock - Rock n Roll /
-- Rockabilly
-- 06040000 | Arts et spectacles - Architecture / Urbanisme | Rock - Rock psychédelique
-- 07010000 | Religion & Esotérisme - Religion généralités  | Métal - Hard Rock /
-- Heavy Metal
-- 07020000 | Religion & Esotérisme - Christianisme         | Métal - Metal / Fusion
-- 08010000 | Entreprise, éco & droit - Sciences éco.       | Alternatif - Punk /
-- Hardcore
-- 08030000 | Entreprise, éco & droit - Droit               | Alternatif - Rock
-- indépendant
-- 09020000 | Sciences humaines - Ethnologie                | Variétés - Variété
-- francophone
-- 10010000 | Sciences & Techniques - Généralités science   | Funk / Soul / RNB - Soul
-- international
-- 10030000 | Sciences & Techniques - Sciences de la vie    | Funk / Soul / RNB - Funk
-- international (approx.)
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

    -- Hard-coded list of shared GTL IDs (present for both books and music,
    -- high volume, diverse genre coverage across the 20 selected codes).
    gtl_selected as (
        select titelive_gtl_id
        from
            unnest(
                [
                    -- Littérature × Musique Classique (6 codes)
                    '01020000',  -- Romans/Nouvelles de genre  ×  Musique de chambre
                    '01030000',  -- Œuvres classiques          ×  Liturgie
                    '01050000',  -- Récit                      ×  Musique baroque
                    '01060000',  -- Biographie littéraire      ×  Classique Symphonie
                    '01080000',  -- Théâtre                    ×  Classique Sonate
                    '01090000',  -- Poésie                     ×  Opéra / Chant lyrique
                    -- Jeunesse × Jazz / Blues (2 codes)
                    '02040000',  -- Lit. Enfants               ×  Jazz Rock / Fusion / Funk
                    '02050000',  -- Lit. Jeunes Adultes        ×  Blues
                    -- BD × Bandes originales (1 code)
                    '03020000',  -- Bandes dessinées           ×  Musique de Séries TV
                    -- Vie pratique × Electro (1 code)
                    '04030000',  -- Arts de la table / Gastro  ×  House Music
                    -- Arts et spectacles × Rock (3 codes)
                    '06010000',  -- Généralités sur l'art      ×  Rock international
                    '06030000',  -- Musique                    ×  Rock n Roll / Rockabilly
                    '06040000',  -- Architecture / Urbanisme   ×  Rock psychédelique
                    -- Religion × Métal (2 codes)
                    '07010000',  -- Religion généralités       ×  Hard Rock / Heavy Metal
                    '07020000',  -- Christianisme              ×  Metal / Fusion
                    -- Entreprise & droit × Alternatif (2 codes)
                    '08010000',  -- Sciences économiques       ×  Punk / Hardcore
                    '08030000',  -- Droit                      ×  Rock indépendant
                    -- Sciences humaines × Variétés (1 code)
                    '09020000',  -- Ethnologie                 ×  Variété francophone
                    -- Sciences & Techniques × Funk / Soul (2 codes)
                    '10010000',  -- Généralités sur la science ×  Soul international
                    '10030000'  -- Sciences de la vie         ×  Funk international
                ]
            ) as titelive_gtl_id
    ),

    -- -------------------------------------------------------------------------
    -- Step 1 : pull offers for the selected GTL IDs
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

    -- One row per item: pick the offer with the best GTL coverage.
    -- Deduplication happens FIRST, independently of gtl_selected, to guarantee
    -- each item_id appears exactly once before any further filtering.
    offers_deduplicated as (
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
            row_number() over (
                partition by offers.item_id
                order by
                    (offer_metadata.gtl_label_level_1 is not null) desc, offers.offer_id
            )
            = 1
    ),

    -- Then restrict to items whose best GTL is in the selected list.
    offers_with_best_metadata as (
        select od.*
        from offers_deduplicated as od
        inner join gtl_selected on od.gtl_id = gtl_selected.titelive_gtl_id
    ),

    -- -------------------------------------------------------------------------
    -- Step 2 : exactly 500 items per (gtl_id, item_type) cell.
    -- Cells with fewer than 500 distinct items are excluded entirely.
    -- -------------------------------------------------------------------------
    sampled as (
        select *
        from offers_with_best_metadata
        qualify
            -- rank within each (gtl_id, item_type) cell
            row_number() over (
                partition by gtl_id, item_type order by item_id  -- deterministic
            )
            <= 500
    ),

    -- Count distinct items per cell to filter out under-populated cells
    cell_counts as (
        select gtl_id, item_type, count(*) as n from sampled group by gtl_id, item_type
    ),

    -- -------------------------------------------------------------------------
    -- Step 3 : join titelive metadata
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
from sampled as o
-- Exclude cells that don't reach exactly 500 items (both types must be present
-- with enough products, otherwise the GTL is not usable for the test).
inner join
    cell_counts
    on o.gtl_id = cell_counts.gtl_id
    and o.item_type = cell_counts.item_type
    and cell_counts.n = 500
left join product_artist_link using (offer_product_id)
left join artist using (artist_id)
left join book_titelive_metadata using (offer_product_id)
left join music_titelive_metadata using (offer_product_id)
