{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(pre_hook="{{create_humanize_id_function()}}") }}

with
    int_applicative__user as (
        select
            u.user_id,
            u.user_creation_date,
            {{ target_schema }}.humanize_id(u.user_id) as user_humanized_id,  -- noqa: PRS
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
                when
                    u.user_activity in (
                        "Chômeur",
                        "En recherche d'emploi ou chômeur",
                        "Demandeur d'emploi"
                    )
                then "Chômeur, En recherche d'emploi"
                when
                    u.user_activity
                    in ("Apprenti, Alternant, Volontaire en service civique rémunéré")
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
            case

                -- to be beneficiary, user must be born after 2000-04-30
                -- and be at least 15 years old
                when
                    u.user_validated_birth_date is not null
                    and u.user_birth_date >= date("2000-04-30")
                    and u.user_birth_date <= date_sub(current_date, interval 15 year)
                then u.user_validated_birth_date
                when
                    u.user_birth_date >= date("2000-04-30")
                    and u.user_birth_date <= date_sub(current_date, interval 15 year)
                then u.user_birth_date
                else null
            end as user_birth_date,
            u.user_school_type,
            u.user_is_active,
            u.user_role,
            u.user_last_connection_date,
            u.user_is_email_validated,
            u.user_has_seen_pro_tutorials,
            u.user_phone_validation_status,
            u.user_has_validated_email,
            u.user_has_enabled_marketing_push,
            u.user_subscribed_themes,
            u.user_subscribed_themes is not null as is_theme_subscribed,
            current_date as reference_date
        from {{ source("raw", "applicative_database_user") }} as u
        where u.user_role in ("UNDERAGE_BENEFICIARY", "BENEFICIARY", "FREE_BENEFICIARY")
    )

select
    user_id,
    user_creation_date,
    user_humanized_id,
    user_has_enabled_marketing_email,
    user_activity,
    user_civility,
    user_birth_date,
    {{ calculate_exact_age("reference_date", "user_birth_date") }} as user_age,
    user_school_type,
    user_is_active,
    user_role,
    user_last_connection_date,
    user_is_email_validated,
    user_has_seen_pro_tutorials,
    user_phone_validation_status,
    user_has_validated_email,
    user_has_enabled_marketing_push,
    user_subscribed_themes,
    is_theme_subscribed
from int_applicative__user
