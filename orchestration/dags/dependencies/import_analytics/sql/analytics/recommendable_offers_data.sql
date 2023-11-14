WITH booking_numbers AS (
    SELECT
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
        offer.item_id as item_id
    FROM
        `{{ bigquery_clean_dataset }}`.booking booking
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock stock ON booking.stock_id = stock.stock_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data offer ON stock.offer_id = offer.offer_id
    WHERE
        booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        AND NOT booking.booking_is_cancelled
    GROUP BY
        offer.item_id
), 



get_recommendable_offers AS (
    SELECT
        offer.offer_id AS offer_id,
        offer.item_id AS item_id,
        offer.offer_product_id AS product_id,
        offer.venue_id AS venue_id,
        offer.offer_name AS name,
        offer.offer_is_duo AS offer_is_duo,
        enriched_item_metadata.subcategory_id AS subcategory_id,
        enriched_item_metadata.category_id as category,
        enriched_item_metadata.search_group_name,
        offer.URL AS url,
        offer.offer_creation_date AS offer_creation_date,
        stock.stock_beginning_date AS stock_beginning_date,
        offer.last_stock_price AS stock_price,
        offer.titelive_gtl_id AS gtl_id,
        glt_mapping.gtl_label_level_1 AS gtl_l1,
        glt_mapping.gtl_label_level_2 AS gtl_l2,
        glt_mapping.gtl_label_level_3 AS gtl_l3,
        glt_mapping.gtl_label_level_4 AS gtl_l4,
        MAX(item_counts.item_count) as item_count,
        MAX(COALESCE(booking_numbers.booking_number, 0)) AS booking_number,
        MAX(COALESCE(booking_numbers.booking_number_last_7_days, 0)) AS booking_number_last_7_days,
        MAX(COALESCE(booking_numbers.booking_number_last_14_days, 0)) AS booking_number_last_14_days,
        MAX(COALESCE(booking_numbers.booking_number_last_28_days, 0)) AS booking_number_last_28_days,
        MIN(offer.is_national) AS is_national,
        MAX(
            CASE
                WHEN (
                    offer.offer_product_id NOT IN ('3469240')
                    AND enriched_item_metadata.subcategory_id <> 'JEU_EN_LIGNE'
                    AND enriched_item_metadata.subcategory_id <> 'JEU_SUPPORT_PHYSIQUE'
                    AND enriched_item_metadata.subcategory_id <> 'ABO_JEU_VIDEO'
                    AND enriched_item_metadata.subcategory_id <> 'ABO_LUDOTHEQUE'
                    AND (
                        offer.URL IS NULL
                        OR offer.last_stock_price = 0
                        OR enriched_item_metadata.subcategory_id = 'LIVRE_NUMERIQUE'
                        OR enriched_item_metadata.subcategory_id = 'ABO_LIVRE_NUMERIQUE'
                        OR enriched_item_metadata.subcategory_id = 'TELECHARGEMENT_LIVRE_AUDIO'
                        OR enriched_item_metadata.category_id = 'MEDIA'
                    )
                ) THEN TRUE
                ELSE FALSE
            END
        ) AS is_underage_recommendable,
        MIN(forbidden_query.subcategory_id is null)) as is_recommendable,
        MAX(sensitive_offer.item_id is not null) as is_sensitive,
        ANY_VALUE(enriched_item_metadata.offer_type_labels) as offer_type_labels,
        ANY_VALUE(enriched_item_metadata.offer_type_domain) as offer_type_domain,
        ANY_VALUE(enriched_item_metadata.offer_type_id) as offer_type_id,
        ANY_VALUE(enriched_item_metadata.offer_type_label) as offer_type_label,
        ANY_VALUE(enriched_item_metadata.offer_sub_type_id) as offer_sub_type_id,
        ANY_VALUE(enriched_item_metadata.offer_sub_type_label) as offer_sub_type_label,

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
        LEFT JOIN booking_numbers ON booking_numbers.item_id = offer.item_id
        LEFT JOIN (
            SELECT count(*) as item_count,
            offer.item_id as item_id,
            FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data offer
            GROUP BY item_id
        ) item_counts on item_counts.item_id = offer.item_id
        JOIN `{{ bigquery_analytics_dataset }}`.offer_with_mediation om on offer.offer_id=om.offer_id
        LEFT JOIN  `{{ bigquery_analytics_dataset }}`.enriched_item_metadata enriched_item_metadata on offer.item_id = enriched_item_metadata.item_id
        LEFT JOIN `{{ bigquery_raw_dataset }}`.forbidden_offers_recommendation forbidden_offer on 
            offer.item_id = forbidden_offer.item_id
        LEFT JOIN `{{ bigquery_raw_dataset }}`.sensitive_offers_recommendation sensitive_offer on 
            offer.item_id = sensitive_offer.item_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.titelive_gtl_mapping glt_mapping on 
            offer.titelive_gtl_id = glt_mapping.gtl_id
    WHERE
        offer.is_active = TRUE
        AND offer.offer_is_bookable = TRUE
        AND offerer.offerer_is_active = TRUE
        AND offer.offer_validation = 'APPROVED'
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
)
SELECT  * 
FROM get_recommendable_offers 
where is_recommendable 