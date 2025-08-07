{{
    config(
        cluster_by="booking_status",
    )
}}

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
    b.user_age_at_booking,
    b.deposit_id,
    b.deposit_type,
    b.deposit_reform_category,
    b.reimbursed,
    b.booking_intermediary_amount,
    b.booking_rank,
    b.booking_used_date,
    b.booking_used_recredit_type,
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
    s.venue_department_name,
    s.venue_region_name,
    s.venue_city,
    s.venue_epci,
    s.venue_density_label,
    s.venue_macro_density_label,
    s.venue_density_level,
    s.venue_academy_name,
    s.venue_is_permanent,
    s.venue_is_virtual,
    s.offerer_id,
    s.offerer_name,
    s.is_local_authority,
    s.partner_id,
    s.offer_subcategory_id,
    s.physical_goods,
    s.digital_goods,
    s.event,
    s.offer_category_id,
    s.last_stock_price,
    s.item_id,
    s.venue_iris_internal_id,
    s.offer_url,
    s.isbn,
    o.offer_type_label,
    o.offer_sub_type_label,
    ds.diversity_score,
    rank() over (
        partition by b.user_id, s.offer_subcategory_id order by b.booking_created_at
    ) as same_category_booking_rank,
    rank() over (
        partition by b.user_id order by b.booking_created_at asc, b.booking_id asc
    ) as user_booking_rank,
    s.offerer_is_epn
from {{ ref("int_applicative__booking") }} as b
inner join {{ ref("int_global__stock") }} as s on b.stock_id = s.stock_id
left join {{ ref("int_applicative__offer_metadata") }} as o on s.offer_id = o.offer_id
left join {{ ref("int_metric__diversity_score") }} as ds on b.booking_id = ds.booking_id
