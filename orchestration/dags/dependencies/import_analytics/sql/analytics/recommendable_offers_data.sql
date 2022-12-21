WITH get_recommendable_offers AS(
    SELECT
        DISTINCT (offer.offer_id) AS offer_id,
        offer.item_id AS item_id,
        offer.offer_product_id AS product_id,
        offer.venue_id AS venue_id,
        offer.offer_subcategoryId AS subcategory_id,
        subcategories.category_id AS category,
        subcategories.search_group_name AS search_group_name,
        offer.offer_name AS name,
        offer.offer_is_duo AS offer_is_duo,
        offer.movie_type AS movie_type,
        offer.type AS offer_type_id,
        type_ref.label as offer_type_label,
        offer.subType AS offer_sub_type_id,
        sub_type_ref.sub_label as offer_sub_type_label,
        rayon_ref.macro_rayon as macro_rayon,
        offer.URL AS url,
        offer.is_national AS is_national,
        offer.offer_creation_date AS offer_creation_date,
        stock.stock_beginning_date AS stock_beginning_date,
        offer.last_stock_price AS stock_price,
        item_counts.item_count as item_count,
        (
            CASE
                WHEN booking_numbers.booking_number IS NOT NULL THEN booking_numbers.booking_number
                ELSE 0
            END
        ) AS booking_number,
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
        JOIN `{{ bigquery_clean_dataset }}`.subcategories subcategories ON offer.offer_subcategoryId = subcategories.id
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
        LEFT JOIN (SELECT DISTINCT type, label FROM `{{ bigquery_analytics_dataset }}`.offer_types) type_ref
            ON offer.type = cast(type_ref.type as string)
        LEFT JOIN (SELECT DISTINCT sub_type, sub_label FROM `{{ bigquery_analytics_dataset }}`.offer_types) sub_type_ref
            ON offer.subType = cast(sub_type_ref.sub_type as string)
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.macro_rayons AS rayon_ref
            ON offer.rayon = rayon_ref.rayon
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