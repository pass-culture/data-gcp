SELECT 
    user_id,
    user_creation_date,
    COALESCE(
        CASE
            WHEN user_postal_code = '97150' THEN '978'
            WHEN SUBSTRING(user_postal_code, 0, 2) = '97' THEN SUBSTRING(user_postal_code, 0, 3)
            WHEN SUBSTRING(user_postal_code, 0, 2) = '98' THEN SUBSTRING(user_postal_code, 0, 3)
            WHEN SUBSTRING(user_postal_code, 0, 3) in ('200', '201', '209', '205') THEN '2A'
            WHEN SUBSTRING(user_postal_code, 0, 3) in ('202', '206') THEN '2B'
            ELSE SUBSTRING(user_postal_code, 0, 2)
            END, 
            user_department_code
        ) AS user_department_code,
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
    user_has_enabled_marketing_email,
    user_activity,  -- keep to check data quality  ->>> lyceen, etudiant, etc PRO ?
    user_needs_to_fill_cultural_survey,  -- do PRO do that ?
    user_cultural_survey_id,  -- same
    user_cultural_survey_filled_date,  -- same
    user_age,  -- keep to check data quality  ->>> lyceen, etudiant, coherence age ?
    user_school_type  -- same
        
FROM  `{{ bigquery_raw_dataset }}`.applicative_database_user
-- only PRO
WHERE user_role = 'PRO'

