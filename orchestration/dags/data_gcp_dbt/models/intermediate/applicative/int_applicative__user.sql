{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

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
)

SELECT
    user_id,
    user_creation_date,
    {{target_schema}}.humanize_id(user_id) AS user_humanized_id,
    user_has_enabled_marketing_email,
    COALESCE(
    CASE
        WHEN u.user_postal_code = "97150" THEN "978"
        WHEN SUBSTRING(u.user_postal_code, 0, 2) = "97" THEN SUBSTRING(u.user_postal_code, 0, 3)
        WHEN SUBSTRING(u.user_postal_code, 0, 2) = "98" THEN SUBSTRING(u.user_postal_code, 0, 3)
        WHEN SUBSTRING(u.user_postal_code, 0, 3) in ("200", "201", "209", "205") THEN "2A"
        WHEN SUBSTRING(u.user_postal_code, 0, 3) in ("202", "206") THEN "2B"
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
        WHEN user_civility = "Mme" THEN "Mme."
        ELSE user_civility
    END AS user_civility,
    user_school_type,
    user_is_active,
    user_age,
    user_role,
    user_birth_date,
    user_cultural_survey_filled_date,
    CASE WHEN user_role IN ("UNDERAGE_BENEFICIARY", "BENEFICIARY") THEN 1 ELSE 0 END AS is_beneficiary,
    u.iris_internal_id AS user_iris_internal_id,
    u.region_name AS user_region_name
FROM users_with_geo_candidates AS u
WHERE ST_CONTAINS(
        u.iris_shape,
        ST_GEOGPOINT(u.user_longitude, u.user_latitude)
) OR u.iris_shape IS NULL
