WITH user_beneficiary as (
    SELECT 
        user_id,
        user_creation_date,
        -- keep user_postal_code by default.
        COALESCE(
        CASE
            WHEN SUBSTRING(user_postal_code, 0, 2) = '97' THEN SUBSTRING(user_postal_code, 0, 3)
            WHEN SUBSTRING(user_postal_code, 0, 2) = '98' THEN SUBSTRING(user_postal_code, 0, 3)
            WHEN SUBSTRING(user_postal_code, 0, 3) in ('200', '201') THEN '2A'
            WHEN SUBSTRING(user_postal_code, 0, 3) = '202' THEN '2B'
            ELSE SUBSTRING(user_postal_code, 0, 2)
            END, 
            user_department_code
        ) AS user_department_code,
        user_postal_code,
        user_activity,
        user_civility,
        user_school_type,
        user_is_active,
        user_age,
        user_role,
        user_birth_date,
        user_cultural_survey_filled_date
    FROM  `{{ bigquery_raw_dataset }}`.applicative_database_user
    -- only BENEFICIARY
    WHERE user_role IN ('UNDERAGE_BENEFICIARY', 'BENEFICIARY')
)

SELECT * EXCEPT(user_department_code), 
    -- set 99 when user user_creation_date does not match opening phases.
    -- this is due to Support changes in the past which migh lead to misunderstandings.
    CASE 
        -- if user_department is not in "pilote" (2019_02 / 2019_06) phase but has created an account before, set 99.
        WHEN 
            user_department_code not in ("29","34","67","93","973")
            AND date(user_creation_date) < "2019-06-01"
        THEN "99"
        -- if user_department is not in "experimentation" (2019_06 / 2021_05) phase but has created an account before, set 99.
        WHEN 
            user_department_code not in ("29","34","67","93","973","22","25","35","56","58","71","08","84","94")
            AND date(user_creation_date) < "2021-05-01"
        THEN "99"
        ELSE user_department_code 
    END AS user_department_code 
FROM user_beneficiary


