select
    booking_id,
    booking_creation_date,
    booking_created_at,
    booking_quantity,
    booking_amount,
    booking_status,
    booking_is_cancelled,
    booking_is_used,
    booking_cancellation_date,
    booking_cancellation_reason,
    user_id,
    deposit_id,
    deposit_type,
    reimbursed,
    booking_intermediary_amount,
    booking_rank,
    booking_used_date,
    stock_beginning_date,
    stock_id,
    offer_id,
    offer_name,
    venue_name,
    venue_label,
    venue_type_label,
    venue_id,
    venue_postal_code,
    venue_department_code,
    venue_region_name,
    venue_city,
    venue_epci,
    venue_density_label,
    venue_macro_density_label,
    venue_density_level,
    venue_academy_name,
    offerer_id,
    offerer_name,
    partner_id,
    offer_subcategory_id,
    physical_goods,
    digital_goods,
    event,
    offer_category_id,
    user_postal_code,
    user_department_code,
    user_region_name,
    user_city,
    user_epci,
    user_academy_name,
    user_density_label,
    user_macro_density_label,
    user_density_level,
    user_creation_date,
    user_activity,
    user_civility,
    user_age,
    user_birth_date,
    user_is_active,
    user_is_in_qpv,
    user_is_unemployed,
    user_is_priority_public,
    item_id,
    same_category_booking_rank,
    user_booking_rank,
    user_booking_id_rank,
    user_iris_internal_id,
    venue_iris_internal_id,
    offer_url,
    isbn

from {{ ref('int_global__booking') }} as b
WHERE deposit_type is not NULL
    and offer_id is not NULL
    and user_id is not NULL
