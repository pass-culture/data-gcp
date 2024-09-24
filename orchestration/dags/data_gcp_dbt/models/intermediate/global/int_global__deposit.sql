with bookings_grouped_by_deposit as (
    select
        b.deposit_id,
        sum(case when b.booking_is_used then b.booking_intermediary_amount end) as total_actual_amount_spent,
        sum(case when not b.booking_is_cancelled then b.booking_intermediary_amount end) as total_theoretical_amount_spent,
        sum(case when b.digital_goods
                and b.offer_url is not NULL
                and not b.booking_is_cancelled then b.booking_intermediary_amount
        end) as total_theoretical_amount_spent_in_digital_goods,
        min(case when not b.booking_is_cancelled then b.booking_creation_date end) as first_individual_booking_date,
        max(case when not b.booking_is_cancelled then b.booking_creation_date end) as last_individual_booking_date,
        count(case when not b.booking_is_cancelled then b.booking_id end) as total_non_cancelled_individual_bookings,
        count(case when b.booking_intermediary_amount = 0 then b.booking_id end) as total_free_bookings,
        count(case when b.booking_quantity = 2 AND not b.booking_is_cancelled THEN b.booking_id end) as total_non_cancelled_duo_bookings,
        count(distinct case when not b.booking_is_cancelled then b.offer_category_id end) as total_distinct_category_booked,
        count(distinct case when not b.booking_is_cancelled then b.offer_subcategory_id end) as total_distinct_booking_types,
        sum(case when b.physical_goods and b.offer_url is null and not b.booking_is_cancelled then b.booking_intermediary_amount end) as total_theoretical_physical_goods_amount_spent,
        sum(case when not b.booking_is_cancelled
                and b.digital_goods
                and b.offer_url is not null then b.booking_intermediary_amount
        end) as total_theoretical_digital_goods_amount_spent,
        sum(case when event
                and not b.booking_is_cancelled then b.booking_intermediary_amount
        end) as total_theoretical_outings_amount_spent,
        max(case when b.user_booking_rank = 1 then b.offer_subcategory_id end) as first_booking_type,
        max(case when b.user_booking_rank = 1 and b.booking_intermediary_amount = 0 then b.offer_subcategory_id end) as first_paid_booking_type,
        min(case when b.booking_intermediary_amount = 0 then b.booking_creation_date end) as first_paid_booking_date,
        SUM(d.delta_diversification) total_diversification,
        SUM(d.venue_id_diversification) total_venue_id_diversification,
        SUM(d.venue_type_label_diversification) total_venue_type_label_diversification,
        SUM(d.category_diversification) total_category_diversification
    from {{ ref('int_global__booking') }} b
    LEFT JOIN {{ ref("diversification_booking") }} d on b.booking_id = d.booking_id 
    group by b.deposit_id
)

select
    d.deposit_id,
    d.deposit_amount,
    d.user_id,
    d.source,
    d.deposit_creation_date,
    d.deposit_update_date,
    d.deposit_expiration_date,
    d.deposit_type,
    d.deposit_source,
    d.deposit_rank_asc,
    d.deposit_rank_desc,
    bgd.total_actual_amount_spent,
    bgd.total_theoretical_amount_spent,
    bgd.total_theoretical_amount_spent_in_digital_goods,
    bgd.first_individual_booking_date,
    bgd.last_individual_booking_date,
    bgd.total_non_cancelled_individual_bookings,
    bgd.total_non_cancelled_duo_bookings,
    bgd.total_free_bookings,
    bgd.total_distinct_booking_types,
    bgd.total_theoretical_physical_goods_amount_spent,
    bgd.total_theoretical_digital_goods_amount_spent,
    bgd.total_theoretical_outings_amount_spent,
    bgd.first_booking_type,
    bgd.first_paid_booking_type,
    bgd.first_paid_booking_date,
    bgd.total_diversification,
    bgd.total_venue_id_diversification,
    bgd.total_venue_type_label_diversification,
    bgd.total_category_diversification 
from {{ ref("int_applicative__deposit") }} as d
    left join bookings_grouped_by_deposit as bgd on bgd.deposit_id = d.deposit_id
