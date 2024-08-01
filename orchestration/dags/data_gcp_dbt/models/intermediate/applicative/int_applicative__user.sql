{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}


with themes_subscribed as (
    select
        user_id,
        currently_subscribed_themes,
        case when (currently_subscribed_themes is NULL or currently_subscribed_themes = "") then FALSE else TRUE end as is_theme_subscribed
    from {{ source("analytics","app_native_logs") }}
    where technical_message_id = "subscription_update"
    qualify ROW_NUMBER() over (partition by user_id order by partition_date desc) = 1
)

select
    u.user_id,
    u.user_creation_date,
    {{ target_schema }}.humanize_id(u.user_id) as user_humanized_id,
    u.user_has_enabled_marketing_email,
    COALESCE(
        case
            when u.user_postal_code = "97150" then "978"
            when SUBSTRING(u.user_postal_code, 0, 2) = "97" then SUBSTRING(u.user_postal_code, 0, 3)
            when SUBSTRING(u.user_postal_code, 0, 2) = "98" then SUBSTRING(u.user_postal_code, 0, 3)
            when SUBSTRING(u.user_postal_code, 0, 3) in ("200", "201", "209", "205") then "2A"
            when SUBSTRING(u.user_postal_code, 0, 3) in ("202", "206") then "2B"
            else SUBSTRING(u.user_postal_code, 0, 2)
        end,
        u.user_department_code
    ) as user_department_code,
    u.user_postal_code,
    case
        when u.user_activity in ("Alternant", "Apprenti", "Volontaire") then "Apprenti, Alternant, Volontaire en service civique rémunéré"
        when u.user_activity in ("Inactif") then "Inactif (ni en emploi ni au chômage), En incapacité de travailler"
        when u.user_activity in ("Étudiant") then "Etudiant"
        when u.user_activity in ("Chômeur", "En recherche d'emploi ou chômeur","Demandeur d'emploi") then "Chômeur, En recherche d'emploi"
        else u.user_activity
    end as user_activity,
    case
        when u.user_civility in ("M", "M.") then "M."
        when u.user_civility = "Mme" then "Mme."
        else u.user_civility
    end as user_civility,
    u.user_school_type,
    u.user_is_active,
    u.user_age,
    u.user_role,
    u.user_birth_date,
    u.user_address,
    u.user_last_connection_date,
    u.user_is_email_validated,
    u.user_has_seen_pro_tutorials,
    u.user_phone_validation_status,
    u.user_has_validated_email,
    u.user_has_enabled_marketing_push,
    ui.user_iris_internal_id,
    ui.user_region_name,
    ui.user_city,
    ui.user_epci,
    ui.user_academy_name,
    ui.user_density_label,
    ui.user_macro_density_label,
    case when ui.qpv_name is not NULL then TRUE else FALSE end as user_is_in_qpv,
    case when u.user_activity in ("Chômeur", "En recherche d'emploi ou chômeur","Demandeur d'emploi") then TRUE else FALSE end as user_is_unemployed,
    case when
            (
                (ui.qpv_name is not NULL)
                or (u.user_activity in ("Chômeur", "En recherche d'emploi ou chômeur","Demandeur d'emploi"))
                or (ui.user_macro_density_label = "rural")
            )
            then TRUE
        else FALSE
    end as user_is_priority_public,
    currently_subscribed_themes,
    is_theme_subscribed
from {{ source("raw", "applicative_database_user") }} as u
    left join {{ ref("int_api_gouv__address_user_location") }} as ui on ui.user_id = u.user_id
    left join themes_subscribed as ts on ts.user_id = u.user_id
where u.user_role in ("UNDERAGE_BENEFICIARY", "BENEFICIARY")
