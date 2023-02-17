
{{ create_humanize_id_function() }} 

WITH offerer_humanized_id AS (
    SELECT
        offerer_id,
        humanize_id(offerer_id) AS humanized_id
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_offerer
    WHERE
        offerer_id is not NULL
),

related_stocks AS (
    SELECT
        offerer.offerer_id,
        MIN(stock.stock_creation_date) AS first_stock_creation_date
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.venue_id = venue.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
    GROUP BY
        offerer_id
),
related_bookings AS (
    SELECT
        offerer.offerer_id,
        MIN(booking.booking_creation_date) AS first_booking_date
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.venue_id = venue.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.stock_id = stock.stock_id
    GROUP BY
        offerer_id
),

related_offers AS (
    SELECT
        offerer.offerer_id,
        COUNT(offer.offer_id) AS offer_cnt
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.venue_id = venue.venue_id
    GROUP BY
        offerer_id
),

related_non_cancelled_bookings AS (
    SELECT
        offerer.offerer_id,
        COUNT(booking.booking_id) AS no_cancelled_booking_cnt
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.venue_id = venue.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking ON booking.stock_id = stock.stock_id
        AND booking.booking_is_cancelled IS FALSE
    GROUP BY 
        offerer_id
),
offerer_department_code AS (
    SELECT
        offerer.offerer_id,
        CASE
            SUBSTRING(offerer_postal_code, 0, 2)
            WHEN '97' THEN SUBSTRING(offerer_postal_code, 0, 3)
            ELSE SUBSTRING(offerer_postal_code, 0, 2)
        END AS offerer_department_code
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer
    WHERE
        "offerer_postal_code" is not NULL
),

related_venues AS (
    SELECT
        offerer.offerer_id,
        COUNT(venue.venue_id) AS venue_cnt
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue ON offerer.offerer_id = venue.venue_managing_offerer_id
    GROUP BY
        1
),

venues_with_offers AS (
    SELECT
        offerer.offerer_id,
        venue.venue_id,
        count(offer.offer_id) AS count_offers
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue ON offerer.offerer_id = venue.venue_managing_offerer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON venue.venue_id = offer.venue_id
    GROUP BY
        offerer_id,
        venue_id
),

related_venues_with_offer AS (
    SELECT
        offerer_id,
        COUNT(
            CASE
                WHEN count_offers > 0 THEN venue_id
                ELSE NULL
            END
        ) AS venue_with_offer
    FROM
        venues_with_offers
    GROUP BY
        offerer_id
),

current_year_revenue AS (
SELECT
    venue.venue_managing_offerer_id AS offerer_id,
    sum(
        coalesce(booking.booking_quantity, 0) * coalesce(booking.booking_amount, 0)
    ) AS current_year_revenue
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
    JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
    JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
    JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue ON offer.venue_id = venue.venue_id
    AND EXTRACT(
        YEAR
        FROM
            booking.booking_creation_date
    ) = EXTRACT(
        YEAR
        FROM
            current_date
    )
    AND booking.booking_is_used
GROUP BY
    venue.venue_managing_offerer_id
),

bookable_offer_cnt_offerer AS (
    SELECT
        offerer_id,
        COUNT(DISTINCT offer_id) AS offerer_bookable_offer_cnt
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_offer_data
    WHERE
        offer_is_bookable
    GROUP BY
        1
)

SELECT
    offerer.offerer_id,
    offerer.offerer_name,
    offerer.offerer_creation_date,
    offerer.offerer_validation_date,
    related_stocks.first_stock_creation_date,
    related_bookings.first_booking_date,
    related_offers.offer_cnt,
    bookable_offer_cnt_offerer.offerer_bookable_offer_cnt,
    related_non_cancelled_bookings.no_cancelled_booking_cnt,
    offerer_department_code.offerer_department_code,
    region_department.region_name AS offerer_region_name,
    offerer.offerer_siren,
    related_venues.venue_cnt,
    related_venues_with_offer.venue_with_offer,
    offerer_humanized_id.humanized_id AS offerer_humanized_id,
    current_year_revenue.current_year_revenue
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer
    LEFT JOIN related_stocks ON related_stocks.offerer_id = offerer.offerer_id
    LEFT JOIN related_bookings ON related_bookings.offerer_id = offerer.offerer_id
    LEFT JOIN related_offers ON related_offers.offerer_id = offerer.offerer_id
    LEFT JOIN related_non_cancelled_bookings ON related_non_cancelled_bookings.offerer_id = offerer.offerer_id
    LEFT JOIN offerer_department_code ON offerer_department_code.offerer_id = offerer.offerer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department AS region_department ON offerer_department_code.offerer_department_code = region_department.num_dep
    LEFT JOIN related_venues ON related_venues.offerer_id = offerer.offerer_id
    LEFT JOIN related_venues_with_offer ON related_venues_with_offer.offerer_id = offerer.offerer_id
    LEFT JOIN offerer_humanized_id ON offerer_humanized_id.offerer_id = offerer.offerer_id
    LEFT JOIN current_year_revenue ON current_year_revenue.offerer_id = offerer.offerer_id
    LEFT JOIN bookable_offer_cnt_offerer ON bookable_offer_cnt_offerer.offerer_id = offerer.offerer_id
WHERE
    offerer.offerer_validation_status='VALIDATED'
    AND offerer.offerer_is_active;