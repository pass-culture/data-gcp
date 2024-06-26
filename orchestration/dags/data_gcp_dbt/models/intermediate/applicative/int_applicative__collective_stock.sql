WITH collective_bookings_grouped_by_collective_stock AS (
    SELECT
        cb.collective_stock_id,
        COUNT(CASE WHEN cb.collective_booking_status != 'CANCELLED' THEN cb.collective_booking_id END) AS total_non_cancelled_collective_bookings,
        COUNT(cb.collective_booking_id) AS total_collective_bookings,
        COUNT(CASE WHEN cb.collective_booking_status IN ('USED','REIMBURSED')THEN cb.collective_booking_id END) AS total_used_collective_bookings,
        MIN(cb.collective_booking_creation_date) AS first_collective_booking_date,
        MAX(cb.collective_booking_creation_date) AS last_collective_booking_date,
        SUM(CASE WHEN cb.collective_booking_status != 'CANCELLED' THEN cs.collective_stock_price END) AS total_collective_theoretic_revenue,
        SUM(CASE WHEN cb.collective_booking_status IN ('USED','REIMBURSED')THEN cs.collective_stock_price END) AS total_collective_real_revenue,
        SUM(CASE WHEN collective_booking_status IN ('USED','REIMBURSED') AND EXTRACT(YEAR FROM collective_booking_creation_date) = EXTRACT(YEAR FROM current_date) THEN cs.collective_stock_price END) AS total_collective_current_year_real_revenue,
        COUNT(CASE WHEN is_current_educational_year AND cb.collective_booking_status != 'CANCELLED' THEN cb.collective_booking_id END) AS total_current_year_non_cancelled_collective_bookings
    FROM {{ ref('int_applicative__collective_booking') }} AS cb
    LEFT JOIN {{ source('raw','applicative_database_collective_stock') }} AS cs ON cb.collective_stock_id = cs.collective_stock_id
    GROUP BY collective_stock_id
)

SELECT cs.collective_stock_id,
    cs.stock_id,
    cs.collective_stock_creation_date,
    cs.collective_stock_modification_date,
    cs.collective_stock_beginning_date_time,
    cs.collective_offer_id,
    cs.collective_stock_price,
    cs.collective_stock_booking_limit_date_time,
    cs.collective_stock_number_of_tickets,
    cs.collective_stock_price_detail,
    bcs.total_non_cancelled_collective_bookings,
    bcs.total_collective_bookings,
    bcs.total_used_collective_bookings,
    bcs.first_collective_booking_date,
    bcs.last_collective_booking_date,
    bcs.total_collective_theoretic_revenue,
    bcs.total_collective_real_revenue,
    bcs.total_collective_current_year_real_revenue,
    bcs.total_current_year_non_cancelled_collective_bookings,
    (DATE(cs.collective_stock_booking_limit_date_time) > CURRENT_DATE OR cs.collective_stock_booking_limit_date_time IS NULL)
    AND (DATE(cs.collective_stock_beginning_date_time) > CURRENT_DATE OR cs.collective_stock_beginning_date_time IS NULL) AS collective_stock_is_bookable
FROM {{ source('raw','applicative_database_collective_stock') }} AS cs
LEFT JOIN collective_bookings_grouped_by_collective_stock AS bcs ON bcs.collective_stock_id = cs.collective_stock_id
