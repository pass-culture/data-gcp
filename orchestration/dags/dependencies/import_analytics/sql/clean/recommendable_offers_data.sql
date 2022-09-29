CREATE TEMP FUNCTION offer_has_at_least_one_bookable_stock(var_offer_id STRING) RETURNS INT64 AS (
    (
        SELECT
            COUNT(1),
        FROM
            `{{ bigquery_clean_dataset }}.applicative_database_stock` stock
        WHERE
            stock.offer_id = var_offer_id
            AND stock.stock_is_soft_deleted = FALSE
            AND (
                stock.stock_beginning_date > CURRENT_DATE()
                OR stock.stock_beginning_date IS NULL
            )
            AND (
                stock.stock_booking_limit_date > CURRENT_DATE()
                OR stock.stock_booking_limit_date IS NULL
            )
            AND (
                stock.stock_quantity IS NULL
                OR (
                    SELECT
                        GREATEST(
                            stock.stock_quantity - COALESCE(SUM(booking.booking_quantity), 0),
                            0
                        )
                    FROM
                        `{{ bigquery_clean_dataset }}.applicative_database_booking` booking
                    WHERE
                        booking.stock_id = stock.stock_id
                        AND booking.booking_is_cancelled = FALSE
                ) > 0
            )
    )
);

CREATE TEMP FUNCTION offer_has_at_least_one_active_mediation(var_offer_id STRING) RETURNS INT64 AS (
    (
        SELECT
            COUNT(1),
        FROM
            `{{ bigquery_clean_dataset }}.applicative_database_mediation` mediation
        WHERE
            mediation.offerId = var_offer_id
            AND mediation.isActive
            AND mediation.thumbCount > 0
    )
);

WITH get_recommendable_offers AS(
    SELECT
        DISTINCT (offer.offer_id) AS offer_id,
        offer.offer_product_id AS product_id,
        offer.venue_id AS venue_id,
        offer.offer_subcategoryId AS subcategory_id,
        subcategories.category_id AS category,
        subcategories.search_group_name AS search_group_name,
        offer.offer_name AS name,
        offer.offer_url AS url,
        offer.offer_is_national AS is_national,
        offer.offer_creation_date AS offer_creation_date,
        stock.stock_beginning_date AS stock_beginning_date,
        enriched_offer.last_stock_price AS stock_price,
        (
            CASE
                WHEN booking_numbers.booking_number IS NOT NULL THEN booking_numbers.booking_number
                ELSE 0
            END
        ) AS booking_number,
        (
            CASE
                WHEN offer.offer_subcategoryId IN ('LIVRE_PAPIER', 'SEANCE_CINE') THEN CONCAT('product-', offer.offer_product_id)
                ELSE CONCAT('offer-', offer.offer_id)
            END
        ) AS item_id,
        (
            CASE
                WHEN (
                    offer.offer_product_id NOT IN ('3469240')
                    AND offer.offer_subcategoryId <> 'JEU_EN_LIGNE'
                    AND offer.offer_subcategoryId <> 'JEU_SUPPORT_PHYSIQUE'
                    AND offer.offer_subcategoryId <> 'ABO_JEU_VIDEO'
                    AND offer.offer_subcategoryId <> 'ABO_LUDOTHEQUE'
                    AND (
                        offer.offer_url IS NULL
                        OR enriched_offer.last_stock_price = 0
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
        `{{ bigquery_clean_dataset }}.applicative_database_offer` offer
        JOIN `{{ bigquery_clean_dataset }}.subcategories` subcategories ON offer.offer_subcategoryId = subcategories.id
        JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` enriched_offer ON offer.offer_id = enriched_offer.offer_id
        JOIN (
            SELECT
                *
            FROM
                `{{ bigquery_clean_dataset }}.applicative_database_venue` venue
            WHERE
                venue_validation_token IS NULL
        ) venue ON offer.venue_id = venue.venue_id
        JOIN (
            SELECT
                *
            FROM
                `{{ bigquery_clean_dataset }}.applicative_database_offerer` offerer
            WHERE
                offerer_validation_token IS NULL
        ) offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
        LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_stock` stock ON offer.offer_id = stock.offer_id
        LEFT JOIN (
            SELECT
                COUNT(*) AS booking_number,
                offer.offer_product_id
            FROM
                `{{ bigquery_clean_dataset }}.applicative_database_booking` booking
                LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_stock` stock ON booking.stock_id = stock.stock_id
                LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_offer` offer ON stock.offer_id = offer.offer_id
            WHERE
                booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                AND NOT booking.booking_is_cancelled
            GROUP BY
                offer.offer_product_id
        ) booking_numbers ON booking_numbers.offer_product_id = offer.offer_product_id
    WHERE
        offer.offer_is_active = TRUE
        AND (
            (
                SELECT
                    offer_has_at_least_one_active_mediation(offer.offer_id)
            ) = 1
        )
        AND (
            (
                SELECT
                    offer_has_at_least_one_bookable_stock(offer.offer_id)
            ) = 1
        )
        AND offerer.offerer_is_active = TRUE
        AND offer.offer_validation = 'APPROVED'
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
        AND NOT (offer.offer_subcategoryId = 'ACHAT_INSTRUMENT' AND REGEXP_CONTAINS(LOWER(offer.offer_name), r'bon d’achat|bons d’achat'))
        AND NOT (offer.offer_subcategoryId = 'MATERIEL_ART_CREATIF' AND REGEXP_CONTAINS(LOWER(offer.offer_name), r'stabilo|surligneurs'))
        AND offer.offer_product_id NOT IN (SELECT * FROM `{{ bigquery_clean_dataset }}.forbiden_offers_recommendation`)
)
SELECT  * FROM get_recommendable_offers