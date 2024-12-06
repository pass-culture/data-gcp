{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(pre_hook="{{create_humanize_id_function()}}") }}

select
    user_id,
    user_creation_date,
    {{ target_schema }}.humanize_id(user_id) as user_humanized_id,
    user_has_enabled_marketing_email,
    coalesce(
        case
            when user_postal_code = "97150"
            then "978"
            when substring(user_postal_code, 0, 2) = "97"
            then substring(user_postal_code, 0, 3)
            when substring(user_postal_code, 0, 2) = "98"
            then substring(user_postal_code, 0, 3)
            when substring(user_postal_code, 0, 3) in ("200", "201", "209", "205")
            then "2A"
            when substring(user_postal_code, 0, 3) in ("202", "206")
            then "2B"
            else substring(user_postal_code, 0, 2)
        end,
        user_department_code
    ) as user_department_code,
    user_postal_code,
    case
        when user_activity in ("Alternant")
        then "Alternant"
        when user_activity in ("Apprenti")
        then "Apprenti"
        when user_activity in ("Volontaire")
        then "Volontaire en service civique rémunéré"
        when user_activity in ("Inactif")
        then "Inactif (ni en emploi ni au chômage), En incapacité de travailler"
        when user_activity in ("Étudiant")
        then "Etudiant"
        when
            user_activity
            in ("Chômeur", "En recherche d'emploi ou chômeur", "Demandeur d'emploi")
        then "Chômeur, En recherche d'emploi"
        when
            user_activity
            in ("Apprenti, Alternant, Volontaire en service civique rémunéré")
        then null
        else user_activity
    end as user_activity,
    case
        when user_civility in ("M", "M.")
        then "M."
        when user_civility = "Mme"
        then "Mme."
        else user_civility
    end as user_civility,
    user_school_type,
    user_is_active,
    user_age,
    user_role,
    user_birth_date,
    user_address,
    user_last_connection_date,
    user_is_email_validated,
    user_has_seen_pro_tutorials,
    user_phone_validation_status,
    user_has_validated_email,
    user_has_enabled_marketing_push,
    user_subscribed_themes,
    user_subscribed_themes is not null as is_theme_subscribed
from {{ source("raw", "applicative_database_user") }}
where user_role in ("UNDERAGE_BENEFICIARY", "BENEFICIARY")
