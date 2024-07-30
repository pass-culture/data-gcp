{{
    config(
        materialized = "view"
    )
 }}


with bookings_deposit_grouped_by_user as (
    select
        b.user_id,
        count(case when stock_id is not null and offer_name is not null then booking_id end) as total_individual_bookings,
        count(case when not booking_is_cancelled and offer_name is not null then booking_id end) as total_non_cancelled_individual_bookings,
        sum(case when booking_is_used then booking_intermediary_amount end) as total_actual_amount_spent,
        sum(case when not booking_is_cancelled then booking_intermediary_amount end) as total_theoretical_amount_spent,
        min(case when not booking_is_cancelled then booking_created_at end) as first_individual_booking_date,
        max(booking_created_at) as last_individual_booking_date,
        min(case when booking_amount > 0 then booking_creation_date end) as booking_creation_date_first,
        sum(case when physical_goods and offer_url is null and not booking_is_cancelled then booking_intermediary_amount end) as total_theoretical_amount_spent_in_physical_goods,
        sum(case when event
                and not booking_is_cancelled then booking_intermediary_amount
        end) as total_theoretical_amount_spent_in_outings,
        count(distinct case when not booking_is_cancelled then offer_subcategory_id end) as total_distinct_booking_types,
        min(case when not booking_is_cancelled then booking_creation_date end) as first_booking_date,
        max(case when user_booking_id_rank = 1 then offer_subcategory_id end) as first_booking_type,
        min(case when coalesce(booking_amount, 0) > 0 then booking_creation_date end) as first_paid_booking_date,
        max(case when user_booking_rank = 2 and not booking_is_cancelled then booking_creation_date end) as second_booking_date,
        sum(case when deposit_rank_desc = 1
                and not booking_is_cancelled then booking_intermediary_amount
        end) as total_deposit_theoretical_amount_spent,
        sum(case when not booking_is_cancelled
                and deposit_rank_desc = 1
                and booking_is_used
                then booking_intermediary_amount
        end) as total_deposit_actual_amount_spent,
        sum(case when not booking_is_cancelled
                and digital_goods
                and offer_url is not null then booking_intermediary_amount
        end) as total_theoretical_amount_spent_in_digital_goods,
        sum(case when deposit_rank_desc = 1
                and digital_goods
                and offer_url is not null
                and not booking_is_cancelled then booking_intermediary_amount
        end) as total_last_deposit_amount_spent_in_digital_goods,
        max(case when offer_subcategory_id = 'ACTIVATION_THING'
                and booking_used_date is not null then booking_used_date
            else null
        end) as user_activation_date
    from {{ ref('int_global__booking') }} as b
        left join {{ ref('int_applicative__deposit') }} as d on d.deposit_id = b.deposit_id
            and deposit_rank_desc = 1
    group by user_id
),

deposit_grouped_by_user as (
    select
        user_id,
        min(deposit_creation_date) as first_deposit_creation_date,
        min(deposit_amount) as first_deposit_amount,
        max(deposit_amount) as last_deposit_amount,
        max(deposit_expiration_date) as last_deposit_expiration_date,
        sum(deposit_amount) as total_deposit_amount
    from {{ ref('int_applicative__deposit') }}
    group by user_id
),

user_agg_deposit_data as (
    select
        user_id,
        case when last_deposit_amount < 300 then 'GRANT_15_17' else 'GRANT_18' end as current_deposit_type,
        case when first_deposit_amount < 300 then 'GRANT_15_17' else 'GRANT_18' end as first_deposit_type
    from deposit_grouped_by_user
),

ranked_for_bookings_not_canceled as (
    select
        booking_id,
        user_id,
        booking_created_at,
        offer_subcategory_id,
        rank() over (
            partition by
                user_id,
                offer_subcategory_id
            order by booking_created_at
        ) as same_category_booking_rank,
        rank() over (
            partition by user_id
            order by booking_created_at asc
        ) as user_booking_rank
    from {{ ref('int_global__booking') }}
    where booking_is_cancelled is false
),

date_of_bookings_on_third_product as (
    select
        user_id,
        booking_created_at as booking_on_third_product_date
    from ranked_for_bookings_not_canceled
    where same_category_booking_rank = 1
    qualify rank() over (
        partition by user_id
        order by booking_created_at
    ) = 3
),


first_paid_booking_type as (
    select
        user_id,
        offer_subcategory_id as first_paid_booking_type
    from {{ ref('mrt_global__booking') }}
    where booking_amount > 0
    qualify rank() over (
        partition by user_id
        order by
            booking_created_at
    ) = 1
)


select
    u.user_id,
    u.user_department_code,
    u.user_postal_code,
    u.user_city,
    u.user_activity,
    u.user_civility,
    u.user_school_type,
    u.user_cultural_survey_filled_date as first_connection_date,
    u.user_is_active,
    u.user_age,
    u.user_birth_date,
    u.user_has_enabled_marketing_email,
    u.user_iris_internal_id,
    u.user_is_priority_public,
    u.user_is_unemployed,
    u.user_is_in_qpv,
    u.user_epci,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_region_name,
    u.user_academy_name,
    u.user_humanized_id,
    u.currently_subscribed_themes,
    u.is_theme_subscribed,
    dgu.first_deposit_creation_date,
    ud.first_deposit_type,
    dgu.total_deposit_amount,
    ud.current_deposit_type,
    bdgu.first_booking_date,
    bdgu.second_booking_date,
    dbtp.booking_on_third_product_date,
    coalesce(bdgu.total_individual_bookings, 0) as total_individual_bookings,
    coalesce(bdgu.total_non_cancelled_individual_bookings, 0) as total_non_cancelled_individual_bookings,
    bdgu.total_actual_amount_spent,
    bdgu.total_theoretical_amount_spent,
    bdgu.total_theoretical_amount_spent_in_digital_goods,
    bdgu.total_theoretical_amount_spent_in_physical_goods,
    bdgu.total_theoretical_amount_spent_in_outings,
    bdgu.total_deposit_theoretical_amount_spent,
    bdgu.total_last_deposit_amount_spent_in_digital_goods,
    bdgu.total_deposit_actual_amount_spent,
    dgu.last_deposit_amount,
    dgu.last_deposit_amount - bdgu.total_deposit_theoretical_amount_spent as total_theoretical_remaining_credit,
    bdgu.last_individual_booking_date as last_booking_date,
    bdgu.booking_creation_date_first,
    date_diff(bdgu.first_individual_booking_date, dgu.first_deposit_creation_date, day) as days_between_activation_date_and_first_booking_date,
    date_diff(bdgu.booking_creation_date_first, dgu.first_deposit_creation_date, day) as days_between_activation_date_and_first_booking_paid,
    coalesce(user_activation_date, user_creation_date) as user_activation_date,
    bdgu.first_booking_type,
    first_paid_booking_type.first_paid_booking_type,
    bdgu.total_distinct_booking_types,
    ah.action_history_reason as user_suspension_reason,
    dgu.first_deposit_amount as user_deposit_initial_amount,
    dgu.last_deposit_expiration_date as user_deposit_expiration_date,
    case when (
            timestamp(dgu.last_deposit_expiration_date) >= current_timestamp()
            and coalesce(bdgu.total_deposit_actual_amount_spent, 0) < dgu.last_deposit_amount
        )
        and u.user_is_active then true
        else false
    end as user_is_current_beneficiary
from {{ ref('int_applicative__user') }} as u
    left join {{ ref('int_applicative__action_history') }} as ah on ah.user_id = u.user_id and ah.action_history_rk = 1
    inner join user_agg_deposit_data as ud on ud.user_id = u.user_id
    left join deposit_grouped_by_user as dgu on dgu.user_id = u.user_id
    left join bookings_deposit_grouped_by_user as bdgu on bdgu.user_id = u.user_id
    left join date_of_bookings_on_third_product as dbtp on dbtp.user_id = u.user_id
    left join first_paid_booking_type on u.user_id = first_paid_booking_type.user_id
where
    (
        user_is_active
        or action_history_reason = 'upon user request'
    )
