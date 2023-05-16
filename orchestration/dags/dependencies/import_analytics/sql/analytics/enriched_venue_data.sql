{{ create_humanize_id_function() }} 

WITH venue_humanized_id AS (
    SELECT
        venue_id,
        humanize_id(venue_id) AS humanized_id
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue
    WHERE
        venue_id is not NULL
),
offerer_humanized_id AS (
    SELECT
        offerer_id,
        humanize_id(offerer_id) AS humanized_id
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_offerer
    WHERE
        offerer_id is not NULL
),
total_bookings_per_venue AS (
    SELECT
        venue.venue_id,
        count(booking.booking_id) AS total_bookings
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            or offer.booking_email is NULL
        )
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_EVENT', 'ACTIVATION_THING')
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON stock.stock_id = booking.stock_id
    GROUP BY
        venue.venue_id
),
non_cancelled_bookings_per_venue AS (
    SELECT
        venue.venue_id,
        count(booking.booking_id) AS non_cancelled_bookings
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            or offer.booking_email is NULL
        )
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_EVENT', 'ACTIVATION_THING')
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON stock.stock_id = booking.stock_id
        AND NOT booking.booking_is_cancelled
    GROUP BY
        venue.venue_id
),
used_bookings_per_venue AS (
    SELECT
        venue.venue_id,
        count(booking.booking_id) AS used_bookings
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            or offer.booking_email is NULL
        )
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_EVENT', 'ACTIVATION_THING')
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON stock.stock_id = booking.stock_id
        AND booking.booking_is_used
    GROUP BY
        venue.venue_id
),
first_offer_creation_date AS (
    SELECT
        venue.venue_id,
        MIN(offer.offer_creation_date) AS first_offer_creation_date
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            or offer.booking_email is NULL
        )
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_EVENT', 'ACTIVATION_THING')
    GROUP BY
        venue.venue_id
),
last_offer_creation_date AS (
    SELECT
        venue.venue_id,
        MAX(offer.offer_creation_date) AS last_offer_creation_date
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            or offer.booking_email is NULL
        )
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_EVENT', 'ACTIVATION_THING')
    GROUP BY
        venue.venue_id
),
individual_offers_created_per_venue AS (
    SELECT
        venue.venue_id,
        count(offer.offer_id) AS individual_offers_created
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            or offer.booking_email is NULL
        )
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_EVENT', 'ACTIVATION_THING')
    GROUP BY
        venue.venue_id
),
all_collective_offers AS (
    SELECT
        collective_offer_id,
        venue_id
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_collective_offer AS collective_offer
    UNION
    ALL
    SELECT
        collective_offer_id,
        venue_id
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_collective_offer_template AS collective_offer_template
),
collective_offers_created_per_venue AS (
    SELECT
        venue.venue_id,
        count(collective_offer_id) AS collective_offers_created
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN all_collective_offers ON venue.venue_id = all_collective_offers.venue_id
    GROUP BY
        venue.venue_id
),
theoretic_revenue_per_venue AS (
    SELECT
        venue.venue_id,
        COALESCE(
            SUM(
                booking.booking_amount * booking.booking_quantity
            ),
            0
        ) AS theoretic_revenue
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            or offer.booking_email is NULL
        )
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_EVENT', 'ACTIVATION_THING')
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON offer.offer_id = stock.offer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.stock_id = stock.stock_id
        AND NOT booking.booking_is_cancelled
    GROUP BY
        venue.venue_id
),
real_revenue_per_venue AS (
    SELECT
        venue.venue_id,
        COALESCE(
            SUM(
                booking.booking_amount * booking.booking_quantity
            ),
            0
        ) AS real_revenue
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
        AND (
            offer.booking_email != 'jeux-concours@passculture.app'
            or offer.booking_email is NULL
        )
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_EVENT', 'ACTIVATION_THING')
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON offer.offer_id = stock.offer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.stock_id = stock.stock_id
        AND NOT booking.booking_is_cancelled
        AND booking.booking_is_used
    GROUP BY
        venue.venue_id
),
bookable_offer_cnt AS (
    SELECT
        venue_id,
        COUNT(DISTINCT offer_id) AS venue_bookable_offer_cnt
    FROM
        `{{ bigquery_clean_dataset }}`.bookable_offer
    GROUP BY
        1
),
related_bookings AS (
    SELECT
        venue.venue_id,
        MIN(booking.booking_creation_date) AS first_booking_date,
        MAX(booking.booking_creation_date) AS last_booking_date
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.venue_id = venue.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.stock_id = stock.stock_id
    GROUP BY
        venue_id
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
    total_bookings_per_venue.total_bookings,
    non_cancelled_bookings_per_venue.non_cancelled_bookings,
    used_bookings_per_venue.used_bookings,
    first_offer_creation_date.first_offer_creation_date,
    last_offer_creation_date.last_offer_creation_date,
    first_booking_date,
    last_booking_date,
    individual_offers_created_per_venue.individual_offers_created,
    collective_offers_created_per_venue.collective_offers_created,
    bookable_offer_cnt.venue_bookable_offer_cnt,
    theoretic_revenue_per_venue.theoretic_revenue,
    real_revenue_per_venue.real_revenue,
    venue_humanized_id.humanized_id AS venue_humanized_id,
    CONCAT(
        "https://backend.passculture.pro/pc/back-office/venue/edit/?id=",
        venue.venue_id,
        "&url=%2Fpc%2Fback-office%2Fvenue%2F"
    ) AS venue_flaskadmin_link,
    CONCAT(
        "https://backend.passculture.team/backofficev3/pro/venue/",
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
    `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue_label AS venue_label ON venue_label.id = venue.venue_label_id
    LEFT JOIN related_bookings ON related_bookings.venue_id = venue.venue_id
    LEFT JOIN total_bookings_per_venue ON venue.venue_id = total_bookings_per_venue.venue_id
    LEFT JOIN non_cancelled_bookings_per_venue ON venue.venue_id = non_cancelled_bookings_per_venue.venue_id
    LEFT JOIN used_bookings_per_venue ON venue.venue_id = used_bookings_per_venue.venue_id
    LEFT JOIN first_offer_creation_date ON venue.venue_id = first_offer_creation_date.venue_id
    LEFT JOIN last_offer_creation_date ON venue.venue_id = last_offer_creation_date.venue_id
    LEFT JOIN individual_offers_created_per_venue ON venue.venue_id = individual_offers_created_per_venue.venue_id
    LEFT JOIN collective_offers_created_per_venue ON venue.venue_id = collective_offers_created_per_venue.venue_id
    LEFT JOIN theoretic_revenue_per_venue ON venue.venue_id = theoretic_revenue_per_venue.venue_id
    LEFT JOIN real_revenue_per_venue ON venue.venue_id = real_revenue_per_venue.venue_id
    LEFT JOIN venue_humanized_id AS venue_humanized_id ON venue_humanized_id.venue_id = venue.venue_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department AS venue_region_departement ON venue.venue_department_code = venue_region_departement.num_dep
    LEFT JOIN offerer_humanized_id AS offerer_humanized_id ON offerer_humanized_id.offerer_id = venue.venue_managing_offerer_id
    LEFT JOIN bookable_offer_cnt ON bookable_offer_cnt.venue_id = venue.venue_id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue_registration AS venue_registration ON venue.venue_id = venue_registration.venue_id
WHERE
    offerer.offerer_validation_status='VALIDATED'
    AND offerer.offerer_is_active;