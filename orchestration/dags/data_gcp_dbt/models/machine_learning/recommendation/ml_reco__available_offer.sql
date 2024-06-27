
WITH offers_with_mediation AS (
        SELECT offer_id
        FROM {{ ref('int_applicative__mediation') }}
        WHERE is_mediation = 1
    UNION DISTINCT
        SELECT o.offer_id
        FROM {{ ref("int_applicative__offer")}} AS o
        INNER JOIN {{ ref('int_applicative__product') }} AS p on o.offer_product_id = p.id
        WHERE p.is_mediation = 1
), 
item_count AS (
    SELECT 
        item_id, 
        count(distinct offer_id) as item_count
    FROM {{ ref("int_applicative__offer")}}
    GROUP BY item_id
),
booking_numbers AS (
    SELECT
        offer.item_id as item_id,
        SUM(IF(
            booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY), 1, 0
        )) AS booking_number,
        SUM(IF(
            booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY), 1, 0
        )) AS booking_number_last_7_days,
        SUM(IF(
            booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY), 1, 0
        )) AS booking_number_last_14_days,
        SUM(IF(
            booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY), 1, 0
        )) AS booking_number_last_28_days,
    FROM {{ ref('int_applicative__booking') }} booking
    LEFT JOIN {{ ref('int_applicative__stock') }} stock ON booking.stock_id = stock.stock_id
    LEFT JOIN  {{ ref('mrt_global__offer') }}  offer ON stock.offer_id = offer.offer_id
    WHERE
        booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        AND NOT booking.booking_is_cancelled
    GROUP BY
        offer.item_id
), 
embeddings AS (
    SELECT
        ie.item_id,
        ie.semantic_content_hybrid_embedding as semantic_content_embedding,
    FROM
        {{ source('clean', 'item_embeddings') }} ie
    QUALIFY ROW_NUMBER() OVER (
            PARTITION BY item_id
            ORDER by
                extraction_date DESC
        ) = 1
),
embeddings_avg AS (
    SELECT
        item_id,
        AVG(

                cast(e as float64)
        )AS embedding,
    FROM
        embeddings, 
        UNNEST(
                    SPLIT(
                        SUBSTR(
                            semantic_content_embedding,
                            2,
                            LENGTH(semantic_content_embedding) - 2
                        )
                    )
                ) e
    GROUP BY 1
),
get_recommendable_offers AS (
    SELECT
        offer.offer_id AS offer_id,
        offer.item_id AS item_id,
        offer.offer_product_id AS product_id,
        venue.venue_id AS venue_id,
        venue.venue_latitude,
        venue.venue_longitude,
        offer.offer_name AS name,
        offer.offer_is_duo AS offer_is_duo,
        im.subcategory_id AS subcategory_id,
        im.category_id as category,
        im.search_group_name,
        offer.offer_url AS url,
        offer.offer_created_at AS offer_creation_date,
        stock.stock_beginning_date AS stock_beginning_date,
        offer.last_stock_price AS stock_price,
        offer.titelive_gtl_id AS gtl_id,
        im.gtl_type,
        im.gtl_label_level_1 as gtl_l1,
        im.gtl_label_level_2 as gtl_l2,
        im.gtl_label_level_3 as gtl_l3,
        im.gtl_label_level_4 as gtl_l4,
        COALESCE(isem.embedding, 0.0) as semantic_emb_mean,
        MAX(ic.item_count) as item_count,
        MAX(COALESCE(booking_numbers.booking_number, 0)) AS booking_number,
        MAX(COALESCE(booking_numbers.booking_number_last_7_days, 0)) AS booking_number_last_7_days,
        MAX(COALESCE(booking_numbers.booking_number_last_14_days, 0)) AS booking_number_last_14_days,
        MAX(COALESCE(booking_numbers.booking_number_last_28_days, 0)) AS booking_number_last_28_days,
        MIN(offer.is_national) AS is_national,
        MAX(
            CASE
                WHEN (
                    offer.offer_product_id NOT IN ('3469240')
                    AND im.subcategory_id <> 'JEU_EN_LIGNE'
                    AND im.subcategory_id <> 'JEU_SUPPORT_PHYSIQUE'
                    AND im.subcategory_id <> 'ABO_JEU_VIDEO'
                    AND im.subcategory_id <> 'ABO_LUDOTHEQUE'
                    AND (
                        offer.offer_url IS NULL
                        OR offer.last_stock_price = 0
                        OR im.subcategory_id = 'LIVRE_NUMERIQUE'
                        OR im.subcategory_id = 'ABO_LIVRE_NUMERIQUE'
                        OR im.subcategory_id = 'TELECHARGEMENT_LIVRE_AUDIO'
                        OR im.category_id = 'MEDIA'
                    )
                ) THEN TRUE
                ELSE FALSE
            END
        ) AS is_underage_recommendable,
        MAX(COALESCE(forbidden_offer.restrained, False)) as is_restrained,
        MAX(COALESCE(forbidden_offer.blocked, False)) as is_blocked,
        MAX(sensitive_offer.item_id is not null) as is_sensitive,
        ANY_VALUE(im.offer_type_labels) as offer_type_labels,
        ANY_VALUE(im.offer_type_domain) as offer_type_domain,
        ANY_VALUE(im.offer_type_id) as offer_type_id,
        ANY_VALUE(im.offer_type_label) as offer_type_label,
        ANY_VALUE(im.offer_sub_type_id) as offer_sub_type_id,
        ANY_VALUE(im.offer_sub_type_label) as offer_sub_type_label,
        ANY_VALUE(im.cluster_id) AS cluster_id,
        ANY_VALUE(im.topic_id) AS topic_id,
        MAX(
            CASE
                WHEN offer.offer_subcategory_id = 'MUSIQUE_LIVE' THEN 150000
                WHEN offer.offer_subcategory_id = 'MUSIQUE_ENREGISTREE'  THEN 50000
                WHEN offer.offer_subcategory_id = 'SPECTACLE' THEN 100000
                WHEN offer.offer_subcategory_id = 'CINEMA' THEN 50000
                WHEN offer.offer_subcategory_id = 'LIVRE' THEN 50000
                ELSE 50000
            END
        ) as  default_max_distance
    FROM {{ ref('mrt_global__offer') }} offer
    INNER JOIN {{ ref('mrt_global__venue') }} AS venue ON venue.venue_id = offer.venue_id
    INNER JOIN offers_with_mediation om on offer.offer_id=om.offer_id
    INNER JOIN {{ ref('item_metadata') }} AS im on offer.item_id = im.item_id
    LEFT JOIN {{ ref('int_applicative__stock') }} stock ON offer.offer_id = stock.offer_id
    -- TODO: move this to an another query related to item_metadata or more precisely reco_item_metada
    LEFT JOIN booking_numbers ON booking_numbers.item_id = offer.item_id
    LEFT JOIN item_count ic on ic.item_id = offer.item_id
    
    LEFT JOIN {{ ref('ml_reco__restrained_item') }} forbidden_offer on
        offer.item_id = forbidden_offer.item_id
    LEFT JOIN {{ source("raw", "gsheet_ml_recommendation_sensitive_item")}} sensitive_offer on
        offer.item_id = sensitive_offer.item_id
    LEFT JOIN embeddings_avg isem ON isem.item_id = offer.item_id
    WHERE
        offer.is_active = TRUE
        AND offer.offer_is_bookable = TRUE
        AND offer.offer_validation = 'APPROVED'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
SELECT  * 
FROM get_recommendable_offers 
where 
(stock_beginning_date >= CURRENT_DATE) OR (stock_beginning_date IS NULL)
AND NOT is_blocked 
