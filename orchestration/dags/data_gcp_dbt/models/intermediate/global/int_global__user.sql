with themes_subscribed as (
    select
        user_id,
        currently_subscribed_themes,
        case when (currently_subscribed_themes is NULL or currently_subscribed_themes = "") then FALSE else TRUE end as is_theme_subscribed
    from {{ ref("app_native_logs") }}
    where technical_message_id = "subscription_update"
    qualify ROW_NUMBER() over (partition by user_id order by partition_date desc) = 1
),

deposit_grouped_by_user as (
    select
        user_id,
        min(deposit_creation_date) as first_deposit_creation_date,
        min(deposit_amount) as first_deposit_amount,
        max(deposit_amount) as last_deposit_amount,
        max(deposit_expiration_date) as last_deposit_expiration_date,
        sum(deposit_amount) as total_deposit_amount,
        sum(total_non_cancelled_individual_bookings) as total_non_cancelled_individual_bookings,
        sum(total_actual_amount_spent) as total_actual_amount_spent,
        sum(total_theoretical_amount_spent) as total_theoretical_amount_spent,
        min(first_individual_booking_date) as first_individual_booking_date,
        max(last_individual_booking_date) as last_individual_booking_date,
        sum(total_distinct_booking_types) as total_distinct_booking_types,
        sum(total_theoretical_physical_goods_amount_spent) as total_theoretical_physical_goods_amount_spent,
        sum(total_theoretical_digital_goods_amount_spent) as total_theoretical_digital_goods_amount_spent,
        sum(total_theoretical_outings_amount_spent) as total_theoretical_outings_amount_spent,
        max(first_booking_type) as first_booking_type,
        max(first_paid_booking_type) as first_paid_booking_type,
        min(first_paid_booking_date) as first_paid_booking_date,
        sum(case when  deposit_rank_desc = 1 then total_actual_amount_spent end) as total_deposit_actual_amount_spent,
        sum(case when deposit_rank_desc = 1 then total_theoretical_amount_spent_in_digital_goods end) as total_last_deposit_digital_goods_amount_spent,
        max(user_activation_date) as user_activation_date
    from {{ ref('int_global__deposit') }}
    group by user_id
)

select
    u.user_id,
    u.user_department_code,
    u.user_postal_code,
    u.user_city,
    u.user_activity,
    u.user_civility,
    u.user_school_type,
    u.user_is_active,
    u.user_age,
    u.user_creation_date,
    u.user_birth_date,
    u.user_has_enabled_marketing_email,
    u.user_has_enabled_marketing_push,
    u.user_iris_internal_id,
    u.user_is_priority_public,
    u.user_is_unemployed,
    u.user_is_in_qpv,
    u.user_epci,
    u.user_density_label,
    u.user_macro_density_label,
    u.user_density_level,
    u.city_code,
    u.user_region_name,
    u.user_academy_name,
    u.user_humanized_id,
    ts.currently_subscribed_themes,
    CASE WHEN ts.is_theme_subscribed IS NULL THEN FALSE ELSE ts.is_theme_subscribed END AS is_theme_subscribed,
    dgu.first_deposit_creation_date,
    dgu.total_deposit_amount,
    user_activation_date,
    dgu.first_individual_booking_date,
    coalesce(dgu.total_non_cancelled_individual_bookings, 0) as total_non_cancelled_individual_bookings,
    dgu.total_actual_amount_spent,
    dgu.total_theoretical_amount_spent,
    dgu.total_theoretical_digital_goods_amount_spent,
    dgu.total_theoretical_physical_goods_amount_spent,
    dgu.total_theoretical_outings_amount_spent,
    dgu.total_last_deposit_digital_goods_amount_spent,
    dgu.total_deposit_actual_amount_spent,
    dgu.last_deposit_amount,
    case when dgu.last_deposit_amount < 300 then 'GRANT_15_17' else 'GRANT_18' end as current_deposit_type,
    case when dgu.first_deposit_amount < 300 then 'GRANT_15_17' else 'GRANT_18' end as first_deposit_type,
    dgu.last_deposit_amount - dgu.total_theoretical_amount_spent as total_theoretical_remaining_credit,
    dgu.last_individual_booking_date as last_booking_date,
    date_diff(dgu.first_individual_booking_date, dgu.first_deposit_creation_date, day) as days_between_activation_date_and_first_booking_date,
    date_diff(dgu.first_individual_booking_date, dgu.first_deposit_creation_date, day) as days_between_activation_date_and_first_booking_paid,
    dgu.first_booking_type,
    dgu.first_paid_booking_type,
    dgu.total_distinct_booking_types,
    ah.action_history_reason as user_suspension_reason,
    dgu.first_deposit_amount,
    dgu.last_deposit_expiration_date as last_deposit_expiration_date,
    case when (
            timestamp(dgu.last_deposit_expiration_date) >= current_timestamp()
            and coalesce(dgu.total_deposit_actual_amount_spent, 0) < dgu.last_deposit_amount
        )
        and u.user_is_active then true
        else false
    end as user_is_current_beneficiary
from {{ ref('int_applicative__user') }} as u
    left join {{ ref('int_applicative__action_history') }} as ah on ah.user_id = u.user_id and ah.action_history_rk = 1
    left join themes_subscribed as ts on ts.user_id = u.user_id
    left join deposit_grouped_by_user as dgu on dgu.user_id = u.user_id
