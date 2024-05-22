WITH bookings_grouped_by_stock AS (

    SELECT
        stock_id,
        -- garder que un des deux entre total_bookings et total_individual_bookings
        SUM(CASE WHEN booking_is_cancelled = False THEN booking_quantity END) AS total_bookings,
        COUNT(booking_id) AS total_individual_bookings,
        COUNT(CASE WHEN booking_is_cancelled THEN booking_id END) AS total_cancelled_individual_bookings,
        COUNT(CASE WHEN NOT booking_is_cancelled THEN booking_id END) AS total_non_cancelled_individual_bookings,
        COUNT(CASE WHEN  booking_is_used THEN booking_id END) AS total_used_individual_bookings,
        SUM(CASE WHEN NOT booking_is_cancelled THEN booking_intermediary_amount END) AS total_individual_theoretic_revenue,
        SUM(CASE WHEN booking_is_used THEN booking_intermediary_amount END) AS total_individual_real_revenue,
        MIN(booking_creation_date) AS first_individual_booking_date,
        MAX(booking_creation_date) AS last_individual_booking_date,
        COUNT(CASE WHEN booking_rank = 1 THEN booking_id END) AS total_first_bookings
    FROM {{ ref('int_applicative__booking') }}
    GROUP BY stock_id
)

SELECT
    s.stock_id,
    s.stock_id_at_providers,
    s.stock_modified_at_last_provider_date,
    DATE(s.stock_modified_date) AS stock_modified_date,
    s.stock_modified_date AS stock_modified_at,
    s.stock_price,
    s.stock_quantity,
    s.stock_booking_limit_date,
    s.stock_last_provider_id,
    s.offer_id,
    s.stock_is_soft_deleted,
    s.stock_beginning_date,
    s.stock_creation_date,
    s.stock_fields_updated,
    s.price_category_id,
    s.stock_features,
    CASE WHEN s.stock_quantity IS NULL THEN NULL
        ELSE GREATEST( s.stock_quantity - COALESCE(bs.total_bookings, 0),0)
    END AS total_available_stock,
    total_bookings,
    bs.total_individual_bookings,
    bs.total_cancelled_individual_bookings,
    bs.total_non_cancelled_individual_bookings,
    bs.total_used_individual_bookings,
    bs.total_individual_theoretic_revenue,
    bs.total_individual_real_revenue,
    bs.first_individual_booking_date,
    bs.last_individual_booking_date,
    bs.total_first_bookings,
    CASE WHEN ((DATE(s.stock_booking_limit_date) > CURRENT_DATE OR s.stock_booking_limit_date IS NULL)
    AND (DATE(s.stock_beginning_date) > CURRENT_DATE OR s.stock_beginning_date IS NULL)
    -- <> available_stock > 0 OR available_stock is null
    AND (GREATEST(s.stock_quantity - COALESCE(bs.total_bookings, 0),0) > 0 OR s.stock_quantity IS NULL)
    AND NOT s.stock_is_soft_deleted) THEN TRUE ELSE FALSE END AS is_bookable,
    rank() OVER (PARTITION BY s.offer_id ORDER BY s.stock_creation_date DESC, s.stock_id DESC) AS stock_rk
FROM {{ source('raw','applicative_database_stock') }} AS s
LEFT JOIN bookings_grouped_by_stock AS bs ON bs.stock_id = s.stock_id
WHERE TRUE
    {% if is_incremental() %}
    AND stock_modified_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 1 DAY) and DATE("{{ ds() }}")
    {% endif %}
