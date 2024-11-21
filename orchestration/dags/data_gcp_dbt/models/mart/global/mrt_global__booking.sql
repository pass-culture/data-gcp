{{ config(**custom_table_config(cluster_by="offer_id")) }}

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
    b.user_id,
    b.deposit_id,
    b.deposit_type,
    b.reimbursed,
    b.booking_intermediary_amount,
    b.booking_rank,
    b.booking_used_date,
    b.stock_beginning_date,
    b.stock_id,
    b.offer_id,
    b.offer_name,
    b.venue_name,
    b.venue_label,
    b.venue_type_label,
    b.venue_id,
    b.venue_postal_code,
    b.venue_department_code,
    b.venue_department_name,
    b.venue_region_name,
    b.venue_city,
    b.venue_epci,
    b.venue_density_label,
    b.venue_macro_density_label,
    b.venue_density_level,
    b.venue_academy_name,
    b.venue_is_permanent,
    b.venue_is_virtual,
    b.offerer_id,
    b.offerer_name,
    b.partner_id,
    b.offer_subcategory_id,
    b.physical_goods,
    b.digital_goods,
    b.event,
    b.offer_category_id,
    b.item_id,
    b.same_category_booking_rank,
    b.user_booking_rank,
    b.venue_iris_internal_id,
    b.offer_url,
    b.isbn,
    b.offer_type_label,
    b.offer_sub_type_label,
    u.user_iris_internal_id,
    u.user_postal_code,
    u.user_department_code,
    u.user_department_name,
    u.user_region_name,
    u.user_city,
    u.user_epci,
    u.user_academy_name,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_density_level,
    u.user_activation_date,
    u.user_activity,
    u.user_civility,
    u.user_age,
    u.user_birth_date,
    u.user_is_active,
    u.user_is_in_qpv,
    u.user_is_unemployed,
    u.user_is_in_education,
    u.user_is_priority_public,
    u.first_deposit_creation_date
from {{ ref("int_global__booking") }} as b
left join {{ ref("mrt_global__user") }} as u on u.user_id = b.user_id
where deposit_type is not null and b.user_id is not null
