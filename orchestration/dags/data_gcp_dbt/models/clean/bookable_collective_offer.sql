WITH bookings_per_stock AS (
    SELECT
        collective_stock_id,
        COUNT(
            DISTINCT CASE
                WHEN collective_booking_status NOT IN ('CANCELLED') THEN collective_booking_id
                ELSE NULL
            END
        ) AS collective_booking_stock_no_cancelled_cnt
    FROM
        {{ source('raw','applicative_database_collective_booking') }} AS collective_booking
    GROUP BY
        collective_stock_id
)



SELECT
    collective_offer.collective_offer_id
    ,collective_offer.venue_id
    ,venue.venue_managing_offerer_id AS offerer_id
FROM {{ source('raw','applicative_database_collective_stock') }} AS collective_stock
JOIN {{ source('raw','applicative_database_collective_offer') }} AS collective_offer
    ON collective_stock.collective_offer_id = collective_offer.collective_offer_id
LEFT JOIN {{ source('raw','applicative_database_venue') }} AS venue
    ON collective_offer.venue_id = venue.venue_id
LEFT JOIN bookings_per_stock ON bookings_per_stock.collective_stock_id = collective_stock.collective_stock_id
WHERE collective_offer.collective_offer_is_active
AND (DATE(collective_stock.collective_stock_booking_limit_date_time) > CURRENT_DATE
     OR collective_stock.collective_stock_booking_limit_date_time IS NULL)
AND ( DATE( collective_stock.collective_stock_beginning_date_time ) > CURRENT_DATE
     OR collective_stock.collective_stock_beginning_date_time IS NULL)
UNION ALL
SELECT
    template.collective_offer_id
    ,template.venue_id
    ,venue.venue_managing_offerer_id AS offerer_id
FROM
    {{ source('raw','applicative_database_collective_offer_template') }} AS template
    JOIN {{ source('raw','applicative_database_venue') }} AS venue ON venue.venue_id = template.venue_id