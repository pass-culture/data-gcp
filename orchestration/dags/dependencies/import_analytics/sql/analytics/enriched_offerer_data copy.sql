
{{ create_humanize_id_function() }} 

WITH offerer_humanized_id AS (
    SELECT
        offerer_id,
        humanize_id(offerer_id) AS humanized_id
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_offerer
    WHERE
        offerer_id is not NULL
),

individual_bookings_per_offerer AS (
    SELECT
        venue.venue_managing_offerer_id AS offerer_id,
        count(booking.booking_id) AS total_individual_bookings
        ,COUNT(CASE WHEN NOT booking.booking_is_cancelled THEN booking.booking_id ELSE NULL END) AS non_cancelled_individual_bookings
        ,COUNT(CASE WHEN  booking.booking_is_used THEN booking.booking_id ELSE NULL END) AS used_individual_bookings
        ,COALESCE(SUM(CASE WHEN NOT booking.booking_is_cancelled THEN booking.booking_intermediary_amount ELSE NULL END),0) AS individual_theoretic_revenue
        ,COALESCE(SUM(CASE WHEN booking.booking_is_used THEN booking.booking_intermediary_amount ELSE NULL END),0) AS individual_real_revenue
        ,COALESCE(SUM(CASE WHEN booking.booking_is_used AND EXTRACT(YEAR FROM booking.booking_creation_date) = EXTRACT(YEAR FROM current_date) THEN booking.booking_intermediary_amount ELSE NULL END),0) AS individual_current_year_real_revenue
        ,MIN(booking.booking_creation_date) AS first_individual_booking_date
        ,MAX(booking.booking_creation_date) AS last_individual_booking_date
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.booking AS booking ON stock.stock_id = booking.stock_id
    GROUP BY
        venue.venue_managing_offerer_id
),

collective_bookings_per_offerer AS (
    SELECT
        collective_booking.offerer_id
        ,COUNT(collective_booking.collective_booking_id) AS total_collective_bookings
         ,COUNT(CASE WHEN collective_booking_status NOT IN ('CANCELLED')THEN collective_booking.collective_booking_id ELSE NULL END) AS non_cancelled_collective_bookings
        ,COUNT(CASE WHEN collective_booking_status IN ('USED','REIMBURSED')THEN collective_booking.collective_booking_id ELSE NULL END) AS used_collective_bookings
        ,COALESCE(SUM(CASE WHEN collective_booking_status NOT IN ('CANCELLED')THEN collective_stock.collective_stock_price ELSE NULL END),0) AS collective_theoretic_revenue
        ,COALESCE(SUM(CASE WHEN collective_booking_status IN ('USED','REIMBURSED') THEN collective_stock.collective_stock_price ELSE NULL END),0) AS collective_real_revenue
        ,COALESCE(SUM(CASE WHEN collective_booking_status IN ('USED','REIMBURSED') AND EXTRACT(YEAR FROM collective_booking.collective_booking_creation_date) = EXTRACT(YEAR FROM current_date) THEN collective_stock.collective_stock_price ELSE NULL END),0) AS collective_current_year_real_revenue
        ,MIN(collective_booking.collective_booking_creation_date) AS first_collective_booking_date
        ,MAX(collective_booking.collective_booking_creation_date) AS last_collective_booking_date
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_collective_booking AS collective_booking
    INNER JOIN `{{ bigquery_clean_dataset }}`.applicative_database_collective_stock AS collective_stock ON collective_stock.collective_stock_id = collective_booking.collective_stock_id
    GROUP BY
        collective_booking.offerer_id
),

individual_offers_per_offerer AS (
    SELECT
        venue.venue_managing_offerer_id AS offerer_id,
        MIN(offer.offer_creation_date) AS first_individual_offer_creation_date,
        MAX(offer.offer_creation_date) AS last_individual_offer_creation_date,
        COUNT(offer.offer_id) AS individual_offers_created
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
                                                                                    AND offer.offer_validation = 'APPROVED'
    GROUP BY
        venue.venue_managing_offerer_id
),

all_collective_offers AS (
    SELECT
        collective_offer_id,
        venue.venue_id,
        venue.venue_managing_offerer_id AS offerer_id,
        collective_offer_creation_date
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_collective_offer AS collective_offer
     JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue ON venue.venue_id = collective_offer.venue_id
                                                                             AND collective_offer.collective_offer_validation = 'APPROVED'
    UNION
    ALL
    SELECT
        collective_offer_id,
        venue.venue_id,
        venue.venue_managing_offerer_id AS offerer_id,
        collective_offer_creation_date
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_collective_offer_template AS collective_offer_template
     JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue ON venue.venue_id = collective_offer_template.venue_id
                                                                            AND collective_offer_template.collective_offer_validation = 'APPROVED'

),

collective_offers_per_offerer AS (
    SELECT
        offerer_id,
        count(collective_offer_id) AS collective_offers_created,
        MIN(collective_offer_creation_date) AS first_collective_offer_creation_date,
        MAX(collective_offer_creation_date) AS last_collective_offer_creation_date
    FROM
        all_collective_offers
    GROUP BY
        offerer_id
),

bookable_individual_offer_cnt AS (
    SELECT
        offerer_id,
        COUNT(DISTINCT offer_id) AS offerer_bookable_individual_offer_cnt
    FROM
        `{{ bigquery_clean_dataset }}`.bookable_offer
    GROUP BY
        1
 ),

 bookable_collective_offer_cnt AS (
    SELECT
        offerer_id,
        COUNT(DISTINCT collective_offer_id) AS offerer_bookable_collective_offer_cnt
    FROM
        `{{ bigquery_clean_dataset }}`.bookable_collective_offer
    GROUP BY
        1
 ),

 bookable_offer_history AS (
 SELECT
    offerer_id
    , MIN(partition_date) AS offerer_first_bookable_offer_date
    , MAX(partition_date) AS offerer_last_bookable_offer_date
FROM `{{ bigquery_analytics_dataset }}`.bookable_venue_history
GROUP BY 1
),

related_stocks AS (
    SELECT
        offerer.offerer_id,
        MIN(stock.stock_creation_date) AS first_stock_creation_date
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_offerer AS offerer
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer ON offer.venue_id = venue.venue_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
    GROUP BY
        offerer_id
),

offerer_department_code AS (
    SELECT
        offerer.offerer_id,
        CASE
            WHEN offerer_postal_code = '97150' THEN '978'
            WHEN SUBSTRING(offerer_postal_code, 0, 2) = '97' THEN SUBSTRING(offerer_postal_code, 0, 3)
            WHEN SUBSTRING(offerer_postal_code, 0, 2) = '98' THEN SUBSTRING(offerer_postal_code, 0, 3)
            WHEN SUBSTRING(offerer_postal_code, 0, 3) in ('200', '201', '209', '205') THEN '2A'
            WHEN SUBSTRING(offerer_postal_code, 0, 3) in ('202', '206') THEN '2B'
        ELSE SUBSTRING(offerer_postal_code, 0, 2)
        END AS offerer_department_code
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_offerer AS offerer
    WHERE
        "offerer_postal_code" is not NULL
),

related_venues AS (
    SELECT
        offerer.offerer_id,
        COUNT(venue.venue_id) AS venue_cnt
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_offerer AS offerer
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue ON offerer.offerer_id = venue.venue_managing_offerer_id
    GROUP BY
        1
),

venues_with_offers AS (
    SELECT
        offerer.offerer_id,
        count(DISTINCT offer.venue_id) AS nb_venue_with_offers
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_offerer AS offerer
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue ON offerer.offerer_id = venue.venue_managing_offerer_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
    GROUP BY
        offerer_id
        )

SELECT
    enriched_user_pro.*,


    bookable_offer_history.offerer_first_bookable_offer_date,
    bookable_offer_history.offerer_last_bookable_offer_date,
    
    offerer_department_code.offerer_department_code,
    region_department.region_name AS offerer_region_name,

    COALESCE(venues_with_offers.nb_venue_with_offers,0) AS venue_with_offer,
    offerer_humanized_id.humanized_id AS offerer_humanized_id,
FROM
    `{{ bigquery_clean_dataset }}`.applicative_database_user_offerer AS enriched_user_pro
    LEFT JOIN individual_bookings_per_offerer ON individual_bookings_per_offerer.offerer_id = offerer.offerer_id
    LEFT JOIN collective_bookings_per_offerer ON collective_bookings_per_offerer.offerer_id = offerer.offerer_id
    LEFT JOIN individual_offers_per_offerer ON individual_offers_per_offerer.offerer_id = offerer.offerer_id
    LEFT JOIN collective_offers_per_offerer ON collective_offers_per_offerer.offerer_id = offerer.offerer_id
    LEFT JOIN related_stocks ON related_stocks.offerer_id = offerer.offerer_id
    LEFT JOIN offerer_department_code ON offerer_department_code.offerer_id = offerer.offerer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department AS region_department ON offerer_department_code.offerer_department_code = region_department.num_dep
    LEFT JOIN related_venues ON related_venues.offerer_id = offerer.offerer_id
    LEFT JOIN venues_with_offers ON venues_with_offers.offerer_id = offerer.offerer_id
    LEFT JOIN offerer_humanized_id ON offerer_humanized_id.offerer_id = offerer.offerer_id
    LEFT JOIN bookable_individual_offer_cnt ON bookable_individual_offer_cnt.offerer_id = offerer.offerer_id
    LEFT JOIN bookable_collective_offer_cnt ON bookable_collective_offer_cnt.offerer_id = offerer.offerer_id
    LEFT JOIN bookable_offer_history ON bookable_offer_history.offerer_id = offerer.offerer_id
LEFT JOIN `{{ bigquery_analytics_dataset }}`.siren_data AS siren_data ON siren_data.siren = offerer.offerer_siren
LEFT JOIN `{{ bigquery_analytics_dataset }}`.siren_data_labels AS siren_data_labels ON siren_data_labels.activitePrincipaleUniteLegale = siren_data.activitePrincipaleUniteLegale
                                            AND CAST(siren_data_labels.categorieJuridiqueUniteLegale AS STRING) = CAST(siren_data.categorieJuridiqueUniteLegale AS STRING)
WHERE
    offerer.offerer_validation_status='VALIDATED'
    AND offerer.offerer_is_active;