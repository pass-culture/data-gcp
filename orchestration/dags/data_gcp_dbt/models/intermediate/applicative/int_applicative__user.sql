{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

SELECT
    u.user_id,
    u.user_creation_date,
    {{target_schema}}.humanize_id(u.user_id) AS user_humanized_id,
    u.user_has_enabled_marketing_email,
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
        WHEN u.user_activity in ("Alternant", "Apprenti", "Volontaire") THEN "Apprenti, Alternant, Volontaire en service civique rémunéré"
        WHEN u.user_activity in ("Inactif") THEN "Inactif (ni en emploi ni au chômage), En incapacité de travailler"
        WHEN u.user_activity in ("Étudiant") THEN "Etudiant"
        WHEN u.user_activity in ("Chômeur", "En recherche d'emploi ou chômeur") THEN "Chômeur, En recherche d'emploi"
        ELSE u.user_activity
    END AS user_activity,
    CASE
        WHEN u.user_civility in ("M", "M.") THEN "M."
        WHEN u.user_civility = "Mme" THEN "Mme."
        ELSE u.user_civility
    END AS user_civility,
    u.user_school_type,
    u.user_is_active,
    u.user_age,
    u.user_role,
    u.user_birth_date,
    u.user_cultural_survey_filled_date,
    CASE WHEN u.user_role IN ("UNDERAGE_BENEFICIARY", "BENEFICIARY") THEN 1 ELSE 0 END AS is_beneficiary,
    ui.user_iris_internal_id,
    ui.user_region_name
FROM {{ source("raw", "applicative_database_user") }} AS u
LEFT JOIN {{ ref("int_api_gouv__user_address_location") }} AS ui ON ui.user_id = u.user_id
