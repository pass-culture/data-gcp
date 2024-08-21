select
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
    u.user_id,
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
    s.venue_type_label,
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
    u.user_density_level,
    u.user_creation_date,
    u.user_activity,
    u.user_civility,
    u.user_age,
    u.user_birth_date,
    u.user_is_active,
    u.user_is_in_qpv,
    u.user_is_unemployed,
    u.user_is_priority_public,
    s.item_id,
    RANK() over (
        partition by
            b.user_id,
            s.offer_subcategory_id
        order by
            b.booking_created_at
    ) as same_category_booking_rank,
    RANK() over (
        partition by b.user_id
        order by
            booking_creation_date asc
    ) as user_booking_rank,

    RANK() over (
        partition by b.user_id
        order by
            booking_creation_date,
            booking_id asc
    ) as user_booking_id_rank,
    u.user_iris_internal_id,
    s.venue_iris_internal_id,
    s.offer_url,
    o.isbn
from {{ ref('int_applicative__booking') }} as b
    left join {{ ref('int_global__stock') }} as s on s.stock_id = b.stock_id
    left join {{ ref('int_global__offer') }} as o on o.offer_id=s.offer_id
    left join {{ ref('int_applicative__user') }} as u on u.user_id = b.user_id
