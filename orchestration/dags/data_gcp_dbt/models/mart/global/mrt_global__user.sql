select
    user_id,
    user_department_code,
    user_department_name,
    user_postal_code,
    user_city,
    user_activity,
    user_civility,
    user_school_type,
    user_is_active,
    user_age,
    user_birth_date,
    user_has_enabled_marketing_email,
    user_has_enabled_marketing_push,
    user_iris_internal_id,
    user_is_priority_public,
    user_is_unemployed,
    user_is_in_education,
    user_is_in_qpv,
    user_epci,
    user_density_label,
    city_code,
    user_macro_density_label,
    user_density_level,
    user_region_name,
    user_academy_name,
    user_humanized_id,
    user_subscribed_themes as currently_subscribed_themes,
    is_theme_subscribed,
    first_deposit_creation_date,
    first_deposit_type,
    total_deposit_amount,
    current_deposit_type,
    first_individual_booking_date,
    total_non_cancelled_individual_bookings,
    total_non_cancelled_duo_bookings,
    total_free_bookings,
    total_actual_amount_spent,
    total_theoretical_amount_spent,
    total_theoretical_digital_goods_amount_spent,
    total_theoretical_physical_goods_amount_spent,
    total_theoretical_outings_amount_spent,
    total_last_deposit_digital_goods_amount_spent,
    total_deposit_actual_amount_spent,
    last_deposit_amount,
    total_theoretical_remaining_credit,
    user_creation_date,
    last_booking_date,
    days_between_activation_date_and_first_booking_date,
    days_between_activation_date_and_first_booking_paid,
    user_activation_date,
    first_booking_type,
    first_paid_booking_type,
    total_distinct_grant_18_booking_types,
    total_distinct_grant_15_17_booking_types,
    user_suspension_reason,
    first_deposit_amount,
    last_deposit_expiration_date,
    user_is_current_beneficiary,
    date_diff(
        date('{{ ds() }}'), cast(user_activation_date as date), day
    ) as user_seniority,
    "user" as entity
from {{ ref("int_global__user") }}
where (user_is_active or user_suspension_reason = 'upon user request')
