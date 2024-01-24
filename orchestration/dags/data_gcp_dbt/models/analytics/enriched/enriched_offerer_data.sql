
{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_dbt_' ~ target_name) %}

WITH offerer_humanized_id AS (
    SELECT
        offerer_id,
         {{target_schema}}.humanize_id(offerer_id) AS humanized_id
    FROM
        {{ source('raw', 'applicative_database_offerer') }}
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
        {{ source('raw', 'applicative_database_venue') }} AS venue
        LEFT JOIN {{ source('raw', 'applicative_database_offer') }} AS offer ON venue.venue_id = offer.venue_id
        LEFT JOIN {{ source('raw', 'applicative_database_stock') }} AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN {{ ref('booking') }} AS booking ON stock.stock_id = booking.stock_id
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
        {{ source('raw', 'applicative_database_collective_booking') }} AS collective_booking
    INNER JOIN {{ source('raw', 'applicative_database_collective_stock') }} AS collective_stock ON collective_stock.collective_stock_id = collective_booking.collective_stock_id
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
        {{ source('raw', 'applicative_database_venue') }} AS venue
        LEFT JOIN {{ source('raw', 'applicative_database_offer') }} AS offer ON venue.venue_id = offer.venue_id
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
        {{ source('raw', 'applicative_database_collective_offer') }} AS collective_offer
     JOIN {{ source('raw', 'applicative_database_venue') }} AS venue ON venue.venue_id = collective_offer.venue_id
                                                                             AND collective_offer.collective_offer_validation = 'APPROVED'
    UNION
    ALL
    SELECT
        collective_offer_id,
        venue.venue_id,
        venue.venue_managing_offerer_id AS offerer_id,
        collective_offer_creation_date
    FROM
        {{ source('raw', 'applicative_database_collective_offer_template') }} AS collective_offer_template
     JOIN {{ source('raw', 'applicative_database_venue') }} AS venue ON venue.venue_id = collective_offer_template.venue_id
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
        {{ ref('bookable_offer') }}
    GROUP BY
        1
 ),

 bookable_collective_offer_cnt AS (
    SELECT
        offerer_id,
        COUNT(DISTINCT collective_offer_id) AS offerer_bookable_collective_offer_cnt
    FROM
        {{ ref('bookable_collective_offer') }}
    GROUP BY
        1
 ),

 bookable_offer_history AS (
 SELECT
    offerer_id
    , MIN(partition_date) AS offerer_first_bookable_offer_date
    , MAX(partition_date) AS offerer_last_bookable_offer_date
FROM {{ ref('bookable_venue_history') }}
GROUP BY 1
),

related_stocks AS (
    SELECT
        offerer.offerer_id,
        MIN(stock.stock_creation_date) AS first_stock_creation_date
    FROM
        {{ source('raw', 'applicative_database_offerer') }} AS offerer
        LEFT JOIN {{ source('raw', 'applicative_database_venue') }} AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
        LEFT JOIN {{ source('raw', 'applicative_database_offer') }} AS offer ON offer.venue_id = venue.venue_id
        LEFT JOIN {{ source('raw', 'applicative_database_stock') }} AS stock ON stock.offer_id = offer.offer_id
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
        {{ source('raw', 'applicative_database_offerer') }} AS offerer
    WHERE
        "offerer_postal_code" is not NULL
),

related_venues AS (
    SELECT
        offerer.offerer_id
        ,COUNT(DISTINCT venue_id) AS total_venues_managed
        ,COALESCE(COUNT(DISTINCT CASE WHEN NOT venue_is_virtual THEN venue_id ELSE NULL END),0) AS physical_venues_managed
        ,COALESCE(COUNT(DISTINCT CASE WHEN venue_is_permanent THEN venue_id ELSE NULL END),0) AS permanent_venues_managed
    FROM
        {{ source('raw', 'applicative_database_offerer') }} AS offerer
        LEFT JOIN {{ source('raw', 'applicative_database_venue') }} AS venue ON offerer.offerer_id = venue.venue_managing_offerer_id
    GROUP BY
        1
),

venues_with_offers AS (
    SELECT
        offerer.offerer_id,
        count(DISTINCT offer.venue_id) AS nb_venue_with_offers
    FROM
        {{ source('raw', 'applicative_database_offerer') }} AS offerer
        LEFT JOIN {{ source('raw', 'applicative_database_venue') }} AS venue ON offerer.offerer_id = venue.venue_managing_offerer_id
        LEFT JOIN {{ source('raw', 'applicative_database_offer') }} AS offer ON venue.venue_id = offer.venue_id
    GROUP BY
        offerer_id
        )

SELECT
    offerer.offerer_id,
    CONCAT("offerer-", offerer.offerer_id) AS partner_id,
    offerer.offerer_name,
    offerer.offerer_creation_date,
    offerer.offerer_validation_date,
    related_stocks.first_stock_creation_date,
    individual_offers_per_offerer.first_individual_offer_creation_date AS offerer_first_individual_offer_creation_date,
    individual_offers_per_offerer.last_individual_offer_creation_date AS offerer_last_individual_offer_creation_date,
    collective_offers_per_offerer.first_collective_offer_creation_date AS offerer_first_collective_offer_creation_date,
    collective_offers_per_offerer.last_collective_offer_creation_date AS offerer_last_collective_offer_creation_date,
    CASE WHEN first_individual_offer_creation_date IS NOT NULL AND first_collective_offer_creation_date IS NOT NULL THEN LEAST(first_collective_offer_creation_date, first_individual_offer_creation_date)
         WHEN first_individual_offer_creation_date IS NOT NULL THEN first_individual_offer_creation_date
         ELSE first_collective_offer_creation_date END AS offerer_first_offer_creation_date,
    CASE WHEN last_individual_offer_creation_date IS NOT NULL AND last_collective_offer_creation_date IS NOT NULL THEN GREATEST(last_collective_offer_creation_date, last_individual_offer_creation_date)
         WHEN last_individual_offer_creation_date IS NOT NULL THEN last_individual_offer_creation_date
         ELSE last_collective_offer_creation_date END AS offerer_last_offer_creation_date,
    bookable_offer_history.offerer_first_bookable_offer_date,
    bookable_offer_history.offerer_last_bookable_offer_date,
    individual_bookings_per_offerer.first_individual_booking_date AS offerer_first_individual_booking_date,
    individual_bookings_per_offerer.last_individual_booking_date AS offerer_last_individual_booking_date,
    collective_bookings_per_offerer.first_collective_booking_date AS offerer_first_collective_booking_date,
    collective_bookings_per_offerer.last_collective_booking_date AS offerer_last_collective_booking_date,
    CASE WHEN first_individual_booking_date IS NOT NULL AND first_collective_booking_date IS NOT NULL THEN LEAST(first_collective_booking_date, first_individual_booking_date)
         WHEN first_individual_booking_date IS NOT NULL THEN first_individual_booking_date
         ELSE first_collective_booking_date END AS first_booking_date,
    CASE WHEN last_individual_booking_date IS NOT NULL AND last_collective_booking_date IS NOT NULL THEN GREATEST(last_collective_booking_date, last_individual_booking_date)
         WHEN last_individual_booking_date IS NOT NULL THEN last_individual_booking_date
         ELSE last_collective_booking_date END AS offerer_last_booking_date,
    COALESCE(individual_offers_per_offerer.individual_offers_created,0) AS offerer_individual_offers_created,
    COALESCE(collective_offers_per_offerer.collective_offers_created,0) AS offerer_collective_offers_created,
    COALESCE(individual_offers_per_offerer.individual_offers_created,0) + COALESCE(collective_offers_per_offerer.collective_offers_created,0) AS offer_cnt,
    COALESCE(bookable_individual_offer_cnt.offerer_bookable_individual_offer_cnt,0) AS offerer_bookable_individual_offer_cnt,
    COALESCE(bookable_collective_offer_cnt.offerer_bookable_collective_offer_cnt,0) AS offerer_bookable_collective_offer_cnt,
    COALESCE(bookable_individual_offer_cnt.offerer_bookable_individual_offer_cnt,0) + COALESCE(bookable_collective_offer_cnt.offerer_bookable_collective_offer_cnt,0) AS offerer_bookable_offer_cnt,
    COALESCE(individual_bookings_per_offerer.non_cancelled_individual_bookings,0) AS offerer_non_cancelled_individual_bookings,
    COALESCE(collective_bookings_per_offerer.non_cancelled_collective_bookings,0) AS offerer_non_cancelled_collective_bookings,
    COALESCE(individual_bookings_per_offerer.non_cancelled_individual_bookings,0) + COALESCE(collective_bookings_per_offerer.non_cancelled_collective_bookings,0) AS no_cancelled_booking_cnt,
    COALESCE(individual_bookings_per_offerer.used_individual_bookings,0) + COALESCE(collective_bookings_per_offerer.used_collective_bookings,0) AS offerer_used_bookings,
    COALESCE(individual_bookings_per_offerer.used_individual_bookings,0) AS offerer_used_individual_bookings,
    COALESCE(collective_bookings_per_offerer.used_collective_bookings,0) AS offerer_used_collective_bookings,
    COALESCE(individual_bookings_per_offerer.individual_theoretic_revenue,0) AS offerer_individual_theoretic_revenue,
    COALESCE(individual_bookings_per_offerer.individual_real_revenue,0) AS offerer_individual_real_revenue,
    COALESCE(collective_bookings_per_offerer.collective_theoretic_revenue,0) AS offerer_collective_theoretic_revenue,
    COALESCE(collective_bookings_per_offerer.collective_real_revenue,0) AS offerer_collective_real_revenue,
    COALESCE(individual_bookings_per_offerer.individual_theoretic_revenue,0) + COALESCE(collective_bookings_per_offerer.collective_theoretic_revenue,0) AS offerer_theoretic_revenue,
    COALESCE(individual_bookings_per_offerer.individual_real_revenue,0) + COALESCE(collective_bookings_per_offerer.collective_real_revenue,0) AS offerer_real_revenue,
    COALESCE(individual_bookings_per_offerer.individual_current_year_real_revenue,0) + COALESCE(collective_bookings_per_offerer.collective_current_year_real_revenue,0) AS current_year_revenue,
    offerer_department_code.offerer_department_code,
    region_department.region_name AS offerer_region_name,
    offerer.offerer_siren,
    siren_data.activitePrincipaleUniteLegale AS legal_unit_business_activity_code,
    label_unite_legale AS legal_unit_business_activity_label,
    siren_data.categorieJuridiqueUniteLegale AS legal_unit_legal_category_code,
    label_categorie_juridique AS legal_unit_legal_category_label,
    siren_data.activitePrincipaleUniteLegale = '84.11Z' AS is_local_authority,
    total_venues_managed,
    physical_venues_managed,
    permanent_venues_managed,
    COALESCE(venues_with_offers.nb_venue_with_offers,0) AS venue_with_offer,
    offerer_humanized_id.humanized_id AS offerer_humanized_id,
FROM
    {{ source('raw', 'applicative_database_offerer') }} AS offerer
    LEFT JOIN individual_bookings_per_offerer ON individual_bookings_per_offerer.offerer_id = offerer.offerer_id
    LEFT JOIN collective_bookings_per_offerer ON collective_bookings_per_offerer.offerer_id = offerer.offerer_id
    LEFT JOIN individual_offers_per_offerer ON individual_offers_per_offerer.offerer_id = offerer.offerer_id
    LEFT JOIN collective_offers_per_offerer ON collective_offers_per_offerer.offerer_id = offerer.offerer_id
    LEFT JOIN related_stocks ON related_stocks.offerer_id = offerer.offerer_id
    LEFT JOIN offerer_department_code ON offerer_department_code.offerer_id = offerer.offerer_id
    LEFT JOIN {{ source('analytics', 'region_department') }} AS region_department ON offerer_department_code.offerer_department_code = region_department.num_dep
    LEFT JOIN related_venues ON related_venues.offerer_id = offerer.offerer_id
    LEFT JOIN venues_with_offers ON venues_with_offers.offerer_id = offerer.offerer_id
    LEFT JOIN offerer_humanized_id ON offerer_humanized_id.offerer_id = offerer.offerer_id
    LEFT JOIN bookable_individual_offer_cnt ON bookable_individual_offer_cnt.offerer_id = offerer.offerer_id
    LEFT JOIN bookable_collective_offer_cnt ON bookable_collective_offer_cnt.offerer_id = offerer.offerer_id
    LEFT JOIN bookable_offer_history ON bookable_offer_history.offerer_id = offerer.offerer_id
LEFT JOIN {{ source('clean', 'siren_data') }} AS siren_data ON siren_data.siren = offerer.offerer_siren
LEFT JOIN {{ source('analytics', 'siren_data_labels') }} AS siren_data_labels ON siren_data_labels.activitePrincipaleUniteLegale = siren_data.activitePrincipaleUniteLegale
                                            AND CAST(siren_data_labels.categorieJuridiqueUniteLegale AS STRING) = CAST(siren_data.categorieJuridiqueUniteLegale AS STRING)
WHERE
    offerer.offerer_validation_status='VALIDATED'
    AND offerer.offerer_is_active