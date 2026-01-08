select
    user_id,
    user_creation_date,
    user_has_enabled_marketing_email,
    user_activity,
    user_civility,
    user_birth_date,
    user_age,
    user_age_at_creation,
    user_school_type,
    user_is_active,
    user_role,
    user_is_email_validated,
    user_phone_validation_status,
    user_has_validated_email,
    user_has_enabled_marketing_push,
    user_subscribed_themes,
    is_theme_subscribed,
    case
        when user_role is null and user_age in (15, 16)
        then '15_16'
        when user_role is null and (user_age >= 17 or user_age <= 14)
        then 'GENERAL_PUBLIC'
        else user_role
    end as user_category
from {{ ref("int_applicative__user") }}
