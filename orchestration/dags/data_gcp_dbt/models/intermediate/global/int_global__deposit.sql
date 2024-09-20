with bookings_grouped_by_deposit as (
    select
        deposit_id,
        sum(case when booking_is_used then booking_intermediary_amount end) as total_actual_amount_spent,
        sum(case when not booking_is_cancelled then booking_intermediary_amount end) as total_theoretical_amount_spent,
        sum(case when digital_goods
                and offer_url is not NULL
                and not booking_is_cancelled then booking_intermediary_amount
        end) as total_theoretical_amount_spent_in_digital_goods,
        min(case when not booking_is_cancelled then booking_creation_date end) as first_individual_booking_date,
        max(case when not booking_is_cancelled then booking_creation_date end) as last_individual_booking_date,
        count(case when not booking_is_cancelled then booking_id end) as total_non_cancelled_individual_bookings,
        count(distinct case when not booking_is_cancelled then offer_subcategory_id end) as total_distinct_booking_types,
        sum(case when physical_goods and offer_url is null and not booking_is_cancelled then booking_intermediary_amount end) as total_theoretical_physical_goods_amount_spent,
        sum(case when not booking_is_cancelled
                and digital_goods
                and offer_url is not null then booking_intermediary_amount
        end) as total_theoretical_digital_goods_amount_spent,
        sum(case when event
                and not booking_is_cancelled then booking_intermediary_amount
        end) as total_theoretical_outings_amount_spent,
        max(case when user_booking_rank = 1 then offer_subcategory_id end) as first_booking_type,
        max(case when user_booking_rank = 1 and last_stock_price = 0 then offer_subcategory_id end) as first_paid_booking_type,
        min(case when last_stock_price = 0 then booking_creation_date end) as first_paid_booking_date,
        max(case when offer_subcategory_id = 'ACTIVATION_THING' and booking_used_date is not null then booking_used_date else null end) as user_activation_date,
    from {{ ref('int_global__booking') }}
    group by deposit_id
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
    bgd.total_distinct_booking_types,
    bgd.total_theoretical_physical_goods_amount_spent,
    bgd.total_theoretical_digital_goods_amount_spent,
    bgd.total_theoretical_outings_amount_spent,
    bgd.first_booking_type,
    bgd.first_paid_booking_type,
    bgd.first_paid_booking_date,
    bgd.user_activation_date
from {{ ref("int_applicative__deposit") }} as d
    left join bookings_grouped_by_deposit as bgd on bgd.deposit_id = d.deposit_id
