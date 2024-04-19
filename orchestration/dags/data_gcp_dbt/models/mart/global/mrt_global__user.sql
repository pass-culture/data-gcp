SELECT
    user_id,
    user_creation_date,
    user_humanized_id,
    user_has_enabled_marketing_email,
    user_department_code,
    user_postal_code,
    user_activity,
    user_civility,
    user_school_type,
    user_is_active,
    user_age,
    user_role,
    user_birth_date,
    user_cultural_survey_filled_date,
    is_beneficiary
FROM {{ ref('int_applicative__user') }}
