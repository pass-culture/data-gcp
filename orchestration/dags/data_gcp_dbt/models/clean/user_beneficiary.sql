{{ config(
    pre_hook="{{create_humanize_id_function()}}"
)}}

{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

WITH users_with_geo_candidates AS (
    SELECT
        u.*,
        ul.latitude AS user_latitude,
        ul.longitude AS user_longitude,
        gi.iris_internal_id,
        gi.region_name,
        gi.iris_shape
    FROM {{ source("raw", "applicative_database_user") }} AS u
    LEFT JOIN {{ source("analytics", "user_locations") }} AS ul USING(user_id)
    LEFT JOIN {{ source('clean', 'geo_iris') }} AS gi
        ON ul.longitude BETWEEN gi.min_longitude AND gi.max_longitude
           AND ul.latitude BETWEEN gi.min_latitude AND gi.max_latitude
),

user_beneficiary as (
    SELECT 
        user_id,
        user_creation_date,
        {{target_schema}}.humanize_id(user_id) AS user_humanized_id,
        user_has_enabled_marketing_email,
        -- keep user_postal_code by default.
        COALESCE(
        CASE
            
            WHEN u.user_postal_code = '97150' THEN '978'
            WHEN SUBSTRING(u.user_postal_code, 0, 2) = '97' THEN SUBSTRING(u.user_postal_code, 0, 3)
            WHEN SUBSTRING(u.user_postal_code, 0, 2) = '98' THEN SUBSTRING(u.user_postal_code, 0, 3)
            WHEN SUBSTRING(u.user_postal_code, 0, 3) in ('200', '201', '209', '205') THEN '2A'
            WHEN SUBSTRING(u.user_postal_code, 0, 3) in ('202', '206') THEN '2B'
            ELSE SUBSTRING(u.user_postal_code, 0, 2)
            END, 
            u.user_department_code
        ) AS user_department_code,
        u.user_postal_code,
        CASE
            WHEN user_activity in ("Alternant", "Apprenti", "Volontaire") THEN "Apprenti, Alternant, Volontaire en service civique rémunéré"
            WHEN user_activity in ("Inactif") THEN "Inactif (ni en emploi ni au chômage), En incapacité de travailler"
            WHEN user_activity in ("Étudiant") THEN "Etudiant"
            WHEN user_activity in ("Chômeur", "En recherche d'emploi ou chômeur") THEN "Chômeur, En recherche d'emploi"
            ELSE user_activity
        END AS user_activity,
        CASE
            WHEN user_civility in ("M", "M.") THEN "M."
            WHEN user_civility IN ("Mme") THEN "Mme"
            ELSE user_civility
        END AS user_civility,
        user_school_type,
        user_is_active,
        user_age,
        user_role,
        user_birth_date,
        user_cultural_survey_filled_date,
        u.iris_internal_id AS user_iris_internal_id
    FROM users_with_geo_candidates AS u
    WHERE ( ST_CONTAINS(
        u.iris_shape,
        ST_GEOGPOINT(u.user_longitude, u.user_latitude)
        ) OR u.iris_shape IS NULL
    )
    -- only BENEFICIARY
    AND  user_role IN ('UNDERAGE_BENEFICIARY', 'BENEFICIARY')
),
ranked_bookings AS (
    SELECT
        booking.user_id,
        offer.offer_subcategoryId,
        booking_used_date,
        RANK() OVER (
            PARTITION BY booking.user_id
            ORDER BY
                booking.booking_creation_date ASC,
                booking.booking_id ASC
        ) AS rank_
    FROM {{ source('raw', 'applicative_database_booking') }} AS booking
    JOIN {{ source('raw', 'applicative_database_stock') }} AS stock 
        ON booking.stock_id = stock.stock_id
    JOIN {{ source('raw', 'applicative_database_offer') }} AS offer 
        ON stock.offer_id = offer.offer_id
)

SELECT 
    user_beneficiary.user_id,
    user_creation_date,
    user_humanized_id,
    user_has_enabled_marketing_email,
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
    END AS user_department_code,
    user_postal_code,
    user_activity,
    user_civility,
    user_school_type,
    user_is_active,
    user_age,
    user_role,
    user_birth_date,
    user_cultural_survey_filled_date,
    user_iris_internal_id,
    CASE
        -- get user activation date with fictional offers (early 2019)
        WHEN offer_subcategoryId = 'ACTIVATION_THING'
        AND booking_used_date IS NOT NULL THEN booking_used_date
        ELSE user_creation_date
    END AS user_activation_date
FROM user_beneficiary
LEFT JOIN ranked_bookings 
    ON user_beneficiary.user_id = ranked_bookings.user_id
    AND rank_ = 1

