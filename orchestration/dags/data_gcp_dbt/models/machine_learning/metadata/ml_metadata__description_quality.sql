{{
    config(
        cluster_by="offer_id",
        tags="weekly",
        labels={"schedule": "weekly"},
    )
}}

-- Phase 0 diagnostic for the "Catalogue Description Quality" project.
-- Deterministic (pure BigQuery) per-offer profiling of `offer_description`: it
-- flags the text problems that hurt embedding quality (dirtiness, boilerplate,
-- duplication, thinness) so cleaning ROI can be quantified before any cleaning
-- is built. No model/GPU cost. Grain: one row per bookable offer_id, matching
-- ml_metadata__offer_quality so both can be joined on offer_id.
-- Duplication/templating are measured across distinct item_id (the embedding
-- grain), so the same product sold by many shops is not mistaken for boilerplate.
with
    offers as (
        select
            offer.offer_id,
            offer.item_id,
            offer.offer_product_id,
            offer.offer_category_id,
            offer.offer_subcategory_id,
            trim(cast(offer.offer_description as string)) as offer_description
        from {{ ref("mrt_global__offer") }} as offer
        where offer.offer_is_bookable
    ),

    normalized as (
        select
            *,
            -- Accent/case/punctuation-insensitive key used to detect descriptions
            -- shared verbatim across distinct items (templated boilerplate).
            nullif(
                trim(
                    regexp_replace(
                        lower(
                            regexp_replace(
                                normalize(coalesce(offer_description, ''), nfd),
                                r'\p{M}',
                                ''
                            )
                        ),
                        r'[^a-z0-9]+',
                        ' '
                    )
                ),
                ''
            ) as normalized_description,
            length(offer_description) as description_char_length,
            case
                when offer_description is null or offer_description = ''
                then 0
                else
                    array_length(
                        split(regexp_replace(offer_description, r'\s+', ' '), ' ')
                    )
            end as description_word_count
        from offers
    ),

    description_reuse as (
        -- Reuse measured on distinct items, not offers: the same product sold by
        -- many shops shares one description legitimately (one item_id), so it is
        -- not boilerplate. Only text reused across DIFFERENT items is templated.
        select
            normalized_description,
            count(distinct item_id) as items_sharing_description,
            count(*) as offers_sharing_description
        from normalized
        where normalized_description is not null
        group by normalized_description
    ),

    flagged as (
        select
            normalized.*,
            coalesce(reuse.items_sharing_description, 1) as items_sharing_description,
            coalesce(reuse.offers_sharing_description, 1) as offers_sharing_description,
            (offer_description is null or offer_description = '') as is_missing,
            -- Same 30-char threshold as absence_description in offer_quality.
            (description_char_length <= 30) as is_too_short,
            (description_char_length < 150 or description_word_count < 25) as is_thin,
            regexp_contains(
                offer_description,
                r'<\s*/?\s*[a-zA-Z][^>]*>|&(nbsp|amp|quot|lt|gt|#\d+);'
            ) as has_html,
            regexp_contains(offer_description, r'(?i)(https?://|www\.)') as has_url,
            regexp_contains(
                offer_description, r'[\w.+\-]+@[\w\-]+\.[\w.\-]+'
            ) as has_email,
            regexp_contains(
                offer_description, r'(?:\+33|0)\s*[1-9](?:[\s.\-]*\d{2}){4}'
            ) as has_phone,
            regexp_contains(
                offer_description,
                r'[\x{1F300}-\x{1FAFF}\x{2600}-\x{27BF}\x{2190}-\x{21FF}\x{2B00}-\x{2BFF}]'
            ) as has_emoji,
            regexp_contains(
                offer_description,
                r'(?i)(code\s?promo|bon\s?de\s?r[ée]duction|livraison\s?(offerte|gratuite)|suivez[- ]nous|retrouvez[- ]nous)'
            ) as has_promo_boilerplate,
            regexp_contains(
                offer_description, r'[!?.\-_=*~]{4,}'
            ) as has_punctuation_spam
        from normalized
        left join description_reuse as reuse using (normalized_description)
    ),

    scored as (
        select
            offer_id,
            item_id,
            offer_product_id,
            offer_category_id,
            offer_subcategory_id,
            offer_description,
            normalized_description,
            description_char_length,
            description_word_count,
            items_sharing_description,
            offers_sharing_description,
            is_missing,
            is_too_short,
            is_thin,
            has_html,
            has_url,
            has_email,
            has_phone,
            has_emoji,
            has_promo_boilerplate,
            has_punctuation_spam,
            -- Reuse across distinct items: 2+ items is a duplicate, a large cluster
            -- is a template. Missing/punctuation-only rows have a null normalized key
            -- (count coalesced to 1) so they never count as shared boilerplate.
            (items_sharing_description > 1) as is_duplicated,
            (items_sharing_description >= 50) as is_templated_boilerplate,
            (
                has_html
                or has_url
                or has_email
                or has_phone
                or has_emoji
                or has_promo_boilerplate
                or has_punctuation_spam
            ) as needs_cleaning
        from flagged
    )

select
    *,
    (is_missing or is_thin) as needs_enrichment,
    case
        when is_missing
        then 'missing'
        when is_too_short
        then 'too_short'
        when is_templated_boilerplate
        then 'templated_boilerplate'
        when needs_cleaning
        then 'dirty'
        when is_duplicated
        then 'duplicated'
        when is_thin
        then 'thin'
        else 'ok'
    end as description_quality_tier
from scored
