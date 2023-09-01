SELECT 
    user.user_id,
    user_offerer.offererId as offerer_id,
    user.user_creation_date,
    COALESCE(
        CASE
            WHEN user.user_postal_code = '97150' THEN '978'
            WHEN SUBSTRING(user.user_postal_code, 0, 2) = '97' THEN SUBSTRING(user.user_postal_code, 0, 3)
            WHEN SUBSTRING(user.user_postal_code, 0, 2) = '98' THEN SUBSTRING(user.user_postal_code, 0, 3)
            WHEN SUBSTRING(user.user_postal_code, 0, 3) in ('200', '201', '209', '205') THEN '2A'
            WHEN SUBSTRING(user.user_postal_code, 0, 3) in ('202', '206') THEN '2B'
            ELSE SUBSTRING(user.user_postal_code, 0, 2)
            END, 
            user_department_code
        ) AS user_department_code,
    user.user_postal_code,
    user.user_role,
    user.user_address,
    user.user_city,
    user.user_last_connection_date,
    user.user_is_email_validated,
    user.user_is_active,
    user.user_has_seen_pro_tutorials,
    user.user_phone_validation_status,
    user.user_has_validated_email,
    user.user_has_enabled_marketing_push,
    user.user_has_enabled_marketing_email,
    user.user_activity,  -- keep to check data quality  ->>> lyceen, etudiant, etc PRO ?
    user.user_needs_to_fill_cultural_survey,  -- do PRO do that ?
    user.user_cultural_survey_id,  -- same
    user.user_cultural_survey_filled_date,  -- same
    user.user_age,  -- keep to check data quality  ->>> lyceen, etudiant, coherence age ?
    user.user_school_type  -- same
        
FROM  `{{ bigquery_raw_dataset }}`.applicative_database_user as user
left join `{{ bigquery_raw_dataset }}`.applicative_database_user.user_offerer as user_offerer on user.user_id = user_offerer.userId
-- only PRO
WHERE user_role = 'PRO'

