SELECT
    b.booking_id,
    b.booking_creation_date,
    b.booking_created_at,
    b.booking_quantity,
    b.booking_amount,
    b.booking_status,
    b.booking_is_cancelled,
    b.booking_is_used,
    b.booking_cancellation_date,
    b.booking_cancellation_reason,
    b.user_id,
    b.deposit_id,
    b.deposit_type,
    b.reimbursed,
    b.booking_intermediary_amount,
    b.booking_rank,
    b.booking_used_date,
    s.stock_beginning_date,
    s.stock_id,
    s.offer_id,
    s.offer_name,
    s.venue_name,
    s.venue_label,
    s.venue_type_label, -- venue_type_name
    s.venue_id,
    s.venue_postal_code,
    s.venue_department_code,
    s.venue_region_name,
    s.venue_city,
    s.venue_epci,
    s.venue_density_label,
    s.venue_macro_density_label,
    s.venue_academy_name,
    s.offerer_id,
    s.offerer_name,
    s.partner_id,
    s.offer_subcategory_id,
    s.physical_goods,
    s.digital_goods,
    s.event,
    s.offer_category_id,
    u.user_postal_code,
    u.user_department_code,
    u.user_region_name,
    u.user_city,
    u.user_epci,
    u.user_academy_name,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_creation_date,
    u.user_activity,
    u.user_civility,
    u.user_age,
    u.user_birth_date,
    u.user_is_active,
    s.item_id,
    RANK() OVER (
        PARTITION BY b.user_id,
        s.offer_subcategory_id
        ORDER BY
            b.booking_created_at
    ) AS same_category_booking_rank,
    RANK() OVER (
            PARTITION BY b.user_id
            ORDER BY
                booking_creation_date ASC
        ) AS user_booking_rank,

    RANK() over (
            PARTITION by b.user_id
            order by
                booking_creation_date,
                booking_id ASC
        ) AS user_booking_id_rank,
    u.user_iris_internal_id,
    s.venue_iris_internal_id,
    s.offer_url,
FROM {{ ref('int_applicative__booking') }} AS b
INNER JOIN {{ ref('mrt_global__stock') }} AS s ON s.stock_id = b.stock_id
INNER JOIN {{ ref('mrt_global__user') }} AS u ON u.user_id = b.user_id
WHERE b.deposit_type IS NOT NULL
    AND s.offer_id IS NOT NULL
