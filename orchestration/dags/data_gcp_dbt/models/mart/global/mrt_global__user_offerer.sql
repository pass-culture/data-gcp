SELECT
    offerer_id,
    user_id,
    user_affiliation_rank,
    user_creation_date,
    user_department_code,
    user_postal_code,
    user_role,
    user_address,
    user_city,
    user_last_connection_date,
    user_is_email_validated,
    user_is_active,
    user_has_seen_pro_tutorials,
    user_phone_validation_status,
    user_has_validated_email,
    user_has_enabled_marketing_push,
    user_has_enabled_marketing_email
FROM {{ ref("int_applicative__user_offerer") }}
WHERE user_offerer_validation_status = "VALIDATED"
    AND user_is_active
