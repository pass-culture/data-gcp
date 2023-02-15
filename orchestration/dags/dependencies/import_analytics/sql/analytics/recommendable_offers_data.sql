WITH get_recommendable_offers AS (
    SELECT
        DISTINCT (offer.offer_id) AS offer_id,
        offer.item_id AS item_id,
        offer.offer_product_id AS product_id,
        offer.venue_id AS venue_id,
        
        offer.offer_name AS name,
        offer.offer_is_duo AS offer_is_duo,
        offer.offer_subcategoryId AS subcategory_id,


        enriched_offer_type.category,
        enriched_offer_type.search_group_name,

        enriched_offer_type.offer_type_id,
        enriched_offer_type.offer_sub_type_id,
        -- TODO add this
        enriched_offer_type.offer_type_domain,
        enriched_offer_type.offer_type_label,
        enriched_offer_type.offer_sub_type_label,

        offer.URL AS url,
        offer.is_national AS is_national,
        offer.offer_creation_date AS offer_creation_date,
        stock.stock_beginning_date AS stock_beginning_date,
        offer.last_stock_price AS stock_price,
        item_counts.item_count as item_count,
        COALESCE(booking_numbers.booking_number, 0) AS booking_number,
        (
            CASE
                WHEN (
                    offer.offer_product_id NOT IN ('3469240')
                    AND offer.offer_subcategoryId <> 'JEU_EN_LIGNE'
                    AND offer.offer_subcategoryId <> 'JEU_SUPPORT_PHYSIQUE'
                    AND offer.offer_subcategoryId <> 'ABO_JEU_VIDEO'
                    AND offer.offer_subcategoryId <> 'ABO_LUDOTHEQUE'
                    AND (
                        offer.URL IS NULL
                        OR offer.last_stock_price = 0
                        OR subcategories.id = 'LIVRE_NUMERIQUE'
                        OR subcategories.id = 'ABO_LIVRE_NUMERIQUE'
                        OR subcategories.id = 'TELECHARGEMENT_LIVRE_AUDIO'
                        OR subcategories.category_id = 'MEDIA'
                    )
                ) THEN TRUE
                ELSE FALSE
            END
        ) AS is_underage_recommendable,
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_offer_data offer
        JOIN (
            SELECT
                *
            FROM
                `{{ bigquery_analytics_dataset }}`.enriched_venue_data venue
            WHERE
                venue.offerer_validation_status = 'VALIDATED'
        ) venue ON offer.venue_id = venue.venue_id
        JOIN (
            SELECT
                *
            FROM
                `{{ bigquery_clean_dataset }}`.applicative_database_offerer offerer
            WHERE
                offerer_validation_status='VALIDATED'
        ) offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock stock ON offer.offer_id = stock.offer_id
        LEFT JOIN (
            SELECT
                COUNT(*) AS booking_number,
                offer.item_id as item_id
            FROM
                `{{ bigquery_clean_dataset }}`.applicative_database_booking booking
                LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock stock ON booking.stock_id = stock.stock_id
                LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data offer ON stock.offer_id = offer.offer_id
            WHERE
                booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                AND NOT booking.booking_is_cancelled
            GROUP BY
                offer.item_id
        ) booking_numbers ON booking_numbers.item_id = offer.item_id
        LEFT JOIN (
            SELECT count(*) as item_count,
            offer.item_id as item_id,
            FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data offer
            GROUP BY item_id
        ) item_counts on item_counts.item_id = offer.item_id
        JOIN `{{ bigquery_analytics_dataset }}`.offer_with_mediation om on offer.offer_id=om.offer_id
        LEFT JOIN  `{{ bigquery_analytics_dataset }}`.enriched_offer_type enriched_offer_type on offer.offer_id = enriched_offer_type.item_id
    WHERE
        offer.is_active = TRUE
        AND offer.offer_is_bookable = TRUE
        AND offerer.offerer_is_active = TRUE
        AND offer.offer_validation = 'APPROVED'
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
        AND NOT (offer.offer_subcategoryId = 'ACHAT_INSTRUMENT' AND REGEXP_CONTAINS(LOWER(offer.offer_name), r'bon d’achat|bons d’achat'))
        AND NOT (offer.offer_subcategoryId = 'MATERIEL_ART_CREATIF' AND REGEXP_CONTAINS(LOWER(offer.offer_name), r'stabilo|surligneurs'))
        AND offer.offer_product_id NOT IN (SELECT * FROM `{{ bigquery_clean_dataset }}`.forbiden_offers_recommendation)
)
SELECT  * FROM get_recommendable_offers