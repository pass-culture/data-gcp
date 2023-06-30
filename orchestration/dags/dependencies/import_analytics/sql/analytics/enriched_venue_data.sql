{{ create_humanize_id_function() }} 

WITH venue_humanized_id AS (
    SELECT
        venue_id,
        humanize_id(venue_id) AS humanized_id
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_venue
    WHERE
        venue_id is not NULL
),
offerer_humanized_id AS (
    SELECT
        offerer_id,
        humanize_id(offerer_id) AS humanized_id
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_offerer
    WHERE
        offerer_id is not NULL
),
individual_bookings_per_venue AS (
    SELECT
        venue.venue_id,
        count(booking.booking_id) AS total_individual_bookings
        ,COUNT(CASE WHEN NOT booking.booking_is_cancelled THEN booking.booking_id ELSE NULL END) AS non_cancelled_individual_bookings
        ,COUNT(CASE WHEN  booking.booking_is_used THEN booking.booking_id ELSE NULL END) AS used_individual_bookings
        ,COALESCE(SUM(CASE WHEN NOT booking.booking_is_cancelled THEN booking.booking_intermediary_amount ELSE NULL END),0) AS individual_theoretic_revenue
        ,COALESCE(SUM(CASE WHEN booking.booking_is_used THEN booking.booking_intermediary_amount ELSE NULL END),0) AS individual_real_revenue
        ,MIN(booking.booking_creation_date) AS first_individual_booking_date
        ,MAX(booking.booking_creation_date) AS last_individual_booking_date
    FROM,
        `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.booking AS booking ON stock.stock_id = booking.stock_id
    GROUP BY
        venue.venue_id
),

collective_bookings_per_venue AS (
    SELECT
        collective_booking.venue_id
        ,COUNT(collective_booking.collective_booking_id) AS total_collective_bookings
         ,COUNT(CASE WHEN collective_booking_status NOT IN ('CANCELLED')THEN collective_booking.collective_booking_id ELSE NULL END) AS non_cancelled_collective_bookings
        ,COUNT(CASE WHEN collective_booking_status IN ('USED','REIMBURSED')THEN collective_booking.collective_booking_id ELSE NULL END) AS used_collective_bookings
        ,COALESCE(SUM(CASE WHEN collective_booking_status NOT IN ('CANCELLED')THEN collective_stock.collective_stock_price ELSE NULL END),0) AS collective_theoretic_revenue
        ,COALESCE(SUM(CASE WHEN collective_booking_status IN ('USED','REIMBURSED')THEN collective_stock.collective_stock_price ELSE NULL END),0) AS collective_real_revenue
        ,MIN(collective_booking.collective_booking_creation_date) AS first_collective_booking_date
        ,MAX(collective_booking.collective_booking_creation_date) AS last_collective_booking_date
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_collective_booking AS collective_booking
    INNER JOIN `{{ bigquery_clean_dataset }}`.applicative_database_collective_stock AS collective_stock ON collective_stock.collective_stock_id = collective_booking.collective_stock_id
    GROUP BY
        collective_booking.venue_id
),

individual_offers_per_venue AS (
    SELECT
        venue.venue_id,
        MIN(offer.offer_creation_date) AS first_individual_offer_creation_date,
        MAX(offer.offer_creation_date) AS last_individual_offer_creation_date,
        COUNT(offer.offer_id) AS individual_offers_created
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
    GROUP BY
        venue.venue_id
),

all_collective_offers AS (
    SELECT
        collective_offer_id,
        venue_id,
        collective_offer_creation_date
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_collective_offer AS collective_offer
    UNION
    ALL
    SELECT
        collective_offer_id,
        venue_id,
        collective_offer_creation_date
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_collective_offer_template AS collective_offer_template
),

collective_offers_per_venue AS (
    SELECT
        venue_id,
        count(collective_offer_id) AS collective_offers_created,
        MIN(collective_offer_creation_date) AS first_collective_offer_creation_date,
        MAX(collective_offer_creation_date) AS last_collective_offer_creation_date
    FROM
        all_collective_offers
    GROUP BY 1
),

bookable_individual_offer_cnt AS (
    SELECT
        venue_id,
        COUNT(DISTINCT offer_id) AS venue_bookable_individual_offer_cnt
    FROM
        `{{ bigquery_clean_dataset }}`.bookable_offer
    GROUP BY
        1
 ),

 bookable_collective_offer_cnt AS (
    SELECT
        venue_id,
        COUNT(DISTINCT collective_offer_id) AS venue_bookable_collective_offer_cnt
    FROM
        `{{ bigquery_clean_dataset }}`.bookable_collective_offer
    GROUP BY
        1
 ),

 bookable_offer_history AS (
 SELECT
    venue_id
    , MIN(partition_date) AS venue_first_bookable_offer_date
    , MAX(partition_date) AS venue_last_bookable_offer_date
FROM `{{ bigquery_analytics_dataset }}`.bookable_venue_history
GROUP BY 1
)

SELECT
    venue.venue_id,
    venue.venue_name,
    venue.venue_public_name,
    venue.venue_booking_email,
    venue.venue_address,
    venue.venue_latitude,
    venue.venue_longitude,
    venue.venue_department_code,
    venue.venue_postal_code,
    venue.venue_city,
    venue.venue_siret,
    venue.venue_is_virtual,
    venue.venue_managing_offerer_id,
    venue.venue_creation_date,
    venue.venue_is_permanent,
    offerer.offerer_name,
    offerer.offerer_validation_status,
    venue.venue_type_code AS venue_type_label,
    venue_label.label AS venue_label,
    COALESCE(individual_bookings_per_venue.total_individual_bookings,0) + COALESCE(collective_bookings_per_venue.total_collective_bookings,0) AS total_bookings,
    COALESCE(individual_bookings_per_venue.non_cancelled_individual_bookings,0) AS non_cancelled_individual_bookings,
    COALESCE(collective_bookings_per_venue.non_cancelled_collective_bookings,0) AS non_cancelled_collective_bookings,
    individual_bookings_per_venue.first_individual_booking_date,
    individual_bookings_per_venue.last_individual_booking_date,
    collective_bookings_per_venue.first_collective_booking_date,
    collective_bookings_per_venue.last_collective_booking_date,
    COALESCE(individual_bookings_per_venue.non_cancelled_individual_bookings,0) + COALESCE(collective_bookings_per_venue.non_cancelled_collective_bookings,0) AS non_cancelled_bookings,
    COALESCE(individual_bookings_per_venue.used_individual_bookings,0) + COALESCE(collective_bookings_per_venue.used_collective_bookings,0) AS used_bookings,
    COALESCE(individual_bookings_per_venue.used_individual_bookings,0) AS used_individual_bookings,
    COALESCE(collective_bookings_per_venue.used_collective_bookings,0) AS used_collective_bookings,
    COALESCE(individual_bookings_per_venue.individual_theoretic_revenue,0) AS individual_theoretic_revenue,
    COALESCE(individual_bookings_per_venue.individual_real_revenue,0) AS individual_real_revenue,
    COALESCE(collective_bookings_per_venue.collective_theoretic_revenue,0) AS collective_theoretic_revenue,
    COALESCE(collective_bookings_per_venue.collective_real_revenue,0) AS collective_real_revenue,
    COALESCE(individual_bookings_per_venue.individual_theoretic_revenue,0) + COALESCE(collective_bookings_per_venue.collective_theoretic_revenue,0) AS theoretic_revenue,
    COALESCE(individual_bookings_per_venue.individual_real_revenue,0) + COALESCE(collective_bookings_per_venue.collective_real_revenue,0) AS real_revenue,
    individual_offers_per_venue.first_individual_offer_creation_date,
    individual_offers_per_venue.last_individual_offer_creation_date,
    COALESCE(individual_offers_per_venue.individual_offers_created,0) AS individual_offers_created,
    collective_offers_per_venue.first_collective_offer_creation_date,
    collective_offers_per_venue.last_collective_offer_creation_date,
    COALESCE(collective_offers_per_venue.collective_offers_created,0) AS collective_offers_created,
    COALESCE(individual_offers_per_venue.individual_offers_created,0) + COALESCE(collective_offers_per_venue.collective_offers_created,0) AS total_offers_created,
    bookable_offer_history.venue_first_bookable_offer_date,
    bookable_offer_history.venue_last_bookable_offer_date,
    CASE WHEN first_individual_booking_date IS NOT NULL AND first_collective_booking_date IS NOT NULL THEN LEAST(first_collective_booking_date, first_individual_booking_date)
         WHEN first_individual_booking_date IS NOT NULL THEN first_individual_booking_date
         ELSE first_collective_booking_date END AS first_booking_date,
    CASE WHEN last_individual_booking_date IS NOT NULL AND last_collective_booking_date IS NOT NULL THEN GREATEST(last_collective_booking_date, last_individual_booking_date)
         WHEN last_individual_booking_date IS NOT NULL THEN last_individual_booking_date
         ELSE last_collective_booking_date END AS last_booking_date,
    CASE WHEN first_individual_offer_creation_date IS NOT NULL AND first_collective_offer_creation_date IS NOT NULL THEN LEAST(first_collective_offer_creation_date, first_individual_offer_creation_date)
         WHEN first_individual_offer_creation_date IS NOT NULL THEN first_individual_offer_creation_date
         ELSE first_collective_offer_creation_date END AS first_offer_creation_date,
    CASE WHEN last_individual_offer_creation_date IS NOT NULL AND last_collective_offer_creation_date IS NOT NULL THEN GREATEST(last_collective_offer_creation_date, last_individual_offer_creation_date)
         WHEN last_individual_offer_creation_date IS NOT NULL THEN last_individual_offer_creation_date
         ELSE last_collective_offer_creation_date END AS last_offer_creation_date,
    COALESCE(bookable_individual_offer_cnt.venue_bookable_individual_offer_cnt,0) AS venue_bookable_individual_offer_cnt,
    COALESCE(bookable_collective_offer_cnt.venue_bookable_collective_offer_cnt,0) AS venue_bookable_collective_offer_cnt,
    COALESCE(bookable_individual_offer_cnt.venue_bookable_individual_offer_cnt,0) + COALESCE(bookable_collective_offer_cnt.venue_bookable_collective_offer_cnt,0) AS venue_bookable_offer_cnt,
    venue_humanized_id.humanized_id AS venue_humanized_id,
    CONCAT(
        "https://backoffice.passculture.team/pro/venue/",
        venue.venue_id
    ) AS venue_backofficev3_link,
    venue_region_departement.region_name AS venue_region_name,
    CONCAT(
        'https://passculture.pro/structures/',
        offerer_humanized_id.humanized_id,
        '/lieux/',
        venue_humanized_id.humanized_id
    ) AS venue_pc_pro_link,
    venue_registration.venue_target AS venue_targeted_audience
FROM
    `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue
    LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer AS offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue_label AS venue_label ON venue_label.id = venue.venue_label_id
    LEFT JOIN individual_bookings_per_venue ON individual_bookings_per_venue.venue_id = venue.venue_id
    LEFT JOIN collective_bookings_per_venue ON collective_bookings_per_venue.venue_id = venue.venue_id
    LEFT JOIN individual_offers_per_venue ON individual_offers_per_venue.venue_id = venue.venue_id
    LEFT JOIN collective_offers_per_venue ON collective_offers_per_venue.venue_id = venue.venue_id
    LEFT JOIN venue_humanized_id AS venue_humanized_id ON venue_humanized_id.venue_id = venue.venue_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department AS venue_region_departement ON venue.venue_department_code = venue_region_departement.num_dep
    LEFT JOIN offerer_humanized_id AS offerer_humanized_id ON offerer_humanized_id.offerer_id = venue.venue_managing_offerer_id
    LEFT JOIN bookable_individual_offer_cnt ON bookable_individual_offer_cnt.venue_id = venue.venue_id
    LEFT JOIN bookable_collective_offer_cnt ON bookable_collective_offer_cnt.venue_id = venue.venue_id
    LEFT JOIN bookable_offer_history ON bookable_offer_history.venue_id = venue.venue_id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue_registration AS venue_registration ON venue.venue_id = venue_registration.venue_id
WHERE
    offerer.offerer_validation_status='VALIDATED'
    AND offerer.offerer_is_active;