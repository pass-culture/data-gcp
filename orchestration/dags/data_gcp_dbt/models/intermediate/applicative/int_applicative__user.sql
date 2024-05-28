{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

SELECT
    user_id,
    user_creation_date,
    {{target_schema}}.humanize_id(user_id) AS user_humanized_id,
    user_has_enabled_marketing_email,
    COALESCE(
    CASE
        WHEN user_postal_code = "97150" THEN "978"
        WHEN SUBSTRING(user_postal_code, 0, 2) = "97" THEN SUBSTRING(user_postal_code, 0, 3)
        WHEN SUBSTRING(user_postal_code, 0, 2) = "98" THEN SUBSTRING(user_postal_code, 0, 3)
        WHEN SUBSTRING(user_postal_code, 0, 3) in ("200", "201", "209", "205") THEN "2A"
        WHEN SUBSTRING(user_postal_code, 0, 3) in ("202", "206") THEN "2B"
        ELSE SUBSTRING(user_postal_code, 0, 2)
        END,
        user_department_code
    ) AS user_department_code,
    user_postal_code,
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
    1 as test
FROM {{ source("raw", "applicative_database_user") }}
