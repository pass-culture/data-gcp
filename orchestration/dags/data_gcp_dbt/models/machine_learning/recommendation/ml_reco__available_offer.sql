with offers_with_mediation as (
    select offer_id
    from {{ ref('int_applicative__mediation') }}
    where is_mediation = 1
    union distinct
    select o.offer_id
    from {{ ref("int_applicative__offer") }} as o
        inner join {{ ref('int_applicative__product') }} as p on o.offer_product_id = p.id
    where p.is_mediation = 1
),

get_recommendable_offers as (
    select
        offer.offer_id as offer_id,
        offer.item_id as item_id,
        offer.offer_product_id as product_id,
        venue.venue_id as venue_id,
        venue.venue_latitude,
        venue.venue_longitude,
        offer.offer_name as name,
        offer.offer_is_duo as offer_is_duo,
        im.subcategory_id as subcategory_id,
        im.category_id as category,
        im.search_group_name,
        offer.offer_url as url,
        offer.offer_created_at as offer_creation_date,
        stock.stock_beginning_date as stock_beginning_date,
        offer.last_stock_price as stock_price,
        offer.titelive_gtl_id as gtl_id,
        im.gtl_type,
        im.gtl_label_level_1 as gtl_l1,
        im.gtl_label_level_2 as gtl_l2,
        im.gtl_label_level_3 as gtl_l3,
        im.gtl_label_level_4 as gtl_l4,
        COALESCE(ml_feat.avg_semantic_embedding, 0.0) as semantic_emb_mean,
        MAX(ml_feat.total_offers) as item_count,
        MAX(COALESCE(ml_feat.booking_number_last_14_days, 0)) as booking_number, -- TODO: legacy purposes, to be removed
        MAX(COALESCE(ml_feat.booking_number_last_7_days, 0)) as booking_number_last_7_days,
        MAX(COALESCE(ml_feat.booking_number_last_14_days, 0)) as booking_number_last_14_days,
        MAX(COALESCE(ml_feat.booking_number_last_28_days, 0)) as booking_number_last_28_days,
        ANY_VALUE(ml_feat.cluster_id) as cluster_id,
        ANY_VALUE(ml_feat.topic_id) as topic_id,
        MIN(offer.is_national) as is_national,
        MAX(
            case
                when (
                    offer.offer_product_id not in ('3469240')
                    and im.subcategory_id <> 'JEU_EN_LIGNE'
                    and im.subcategory_id <> 'JEU_SUPPORT_PHYSIQUE'
                    and im.subcategory_id <> 'ABO_JEU_VIDEO'
                    and im.subcategory_id <> 'ABO_LUDOTHEQUE'
                    and (
                        offer.offer_url is NULL
                        or offer.last_stock_price = 0
                        or im.subcategory_id = 'LIVRE_NUMERIQUE'
                        or im.subcategory_id = 'ABO_LIVRE_NUMERIQUE'
                        or im.subcategory_id = 'TELECHARGEMENT_LIVRE_AUDIO'
                        or im.category_id = 'MEDIA'
                    )
                ) then TRUE
                else FALSE
            end
        ) as is_underage_recommendable,
        MAX(COALESCE(forbidden_offer.restrained, FALSE)) as is_restrained,
        MAX(COALESCE(forbidden_offer.blocked, FALSE)) as is_blocked,
        MAX(sensitive_offer.item_id is not NULL) as is_sensitive,
        ANY_VALUE(im.offer_type_labels) as offer_type_labels,
        ANY_VALUE(im.offer_type_domain) as offer_type_domain,
        ANY_VALUE(im.offer_type_id) as offer_type_id,
        ANY_VALUE(im.offer_type_label) as offer_type_label,
        ANY_VALUE(im.offer_sub_type_id) as offer_sub_type_id,
        ANY_VALUE(im.offer_sub_type_label) as offer_sub_type_label,
        MAX(
            case
                when offer.offer_subcategory_id = 'MUSIQUE_LIVE' then 150000
                when offer.offer_subcategory_id = 'MUSIQUE_ENREGISTREE' then 50000
                when offer.offer_subcategory_id = 'SPECTACLE' then 100000
                when offer.offer_subcategory_id = 'CINEMA' then 50000
                when offer.offer_subcategory_id = 'LIVRE' then 50000
                else 50000
            end
        ) as default_max_distance
    from {{ ref('mrt_global__offer') }} offer
        inner join {{ ref('mrt_global__venue') }} as venue on venue.venue_id = offer.venue_id
        inner join offers_with_mediation om on offer.offer_id = om.offer_id
        inner join {{ ref('item_metadata') }} as im on offer.item_id = im.item_id
        left join {{ ref('int_applicative__stock') }} stock on offer.offer_id = stock.offer_id
        left join {{ ref('ml_feat__item_feature_28_day') }} as ml_feat on ml_feat.item_id = offer.item_id
        left join {{ ref('ml_reco__restrained_item') }} forbidden_offer
            on
                offer.item_id = forbidden_offer.item_id
        left join {{ source("raw", "gsheet_ml_recommendation_sensitive_item") }} sensitive_offer on
            offer.item_id = sensitive_offer.item_id
    where
        offer.is_active = TRUE
        and offer.offer_is_bookable = TRUE
        and offer.offer_validation = 'APPROVED'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22
)

select *
from get_recommendable_offers
where
    (stock_beginning_date >= CURRENT_DATE) or (stock_beginning_date is NULL)
    and not is_blocked
