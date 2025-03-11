{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(pre_hook="{{create_humanize_id_function()}}") }}

select
    u.user_id,
    u.user_creation_date,
    {{ target_schema }}.humanize_id(u.user_id) as user_humanized_id,
    u.user_has_enabled_marketing_email,
    case
        when u.user_activity in ("Alternant")
        then "Alternant"
        when u.user_activity in ("Apprenti")
        then "Apprenti"
        when u.user_activity in ("Volontaire")
        then "Volontaire en service civique rémunéré"
        when u.user_activity in ("Inactif")
        then "Inactif (ni en emploi ni au chômage), En incapacité de travailler"
        when u.user_activity in ("Étudiant")
        then "Etudiant"
        when u.user_activity in ("Chômeur", "En recherche d'emploi ou chômeur", "Demandeur d'emploi")
        then "Chômeur, En recherche d'emploi"
        when u.user_activity in ("Apprenti, Alternant, Volontaire en service civique rémunéré")
        then null
        else u.user_activity
    end as user_activity,
    case
        when u.user_civility in ("M", "M.")
        then "M."
        when u.user_civility = "Mme"
        then "Mme."
        else u.user_civility
    end as user_civility,
    u.user_school_type,
    u.user_is_active,
    u.user_age,
    u.user_role,
    u.user_birth_date,
    u.user_last_connection_date,
    u.user_is_email_validated,
    u.user_has_seen_pro_tutorials,
    u.user_phone_validation_status,
    u.user_has_validated_email,
    u.user_has_enabled_marketing_push,
    u.user_subscribed_themes,
    u.user_subscribed_themes is not null as is_theme_subscribed
from {{ source("raw", "applicative_database_user") }} u
where u.user_role in ("UNDERAGE_BENEFICIARY", "BENEFICIARY")
