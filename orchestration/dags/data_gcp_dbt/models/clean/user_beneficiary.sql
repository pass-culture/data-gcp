{{ config(pre_hook="{{create_humanize_id_function()}}") }}

{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

with
    user_beneficiary as (
        select
            user_id,
            user_creation_date,
            {{ target_schema }}.humanize_id(user_id) as user_humanized_id,
            user_has_enabled_marketing_email,
            -- keep user_postal_code by default.
            coalesce(
                case
                    when u.user_postal_code = '97150'
                    then '978'
                    when substring(u.user_postal_code, 0, 2) = '97'
                    then substring(u.user_postal_code, 0, 3)
                    when substring(u.user_postal_code, 0, 2) = '98'
                    then substring(u.user_postal_code, 0, 3)
                    when
                        substring(u.user_postal_code, 0, 3)
                        in ('200', '201', '209', '205')
                    then '2A'
                    when substring(u.user_postal_code, 0, 3) in ('202', '206')
                    then '2B'
                    else substring(u.user_postal_code, 0, 2)
                end,
                u.user_department_code
            ) as user_department_code,
            u.user_postal_code,
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
                    user_activity in (
                        "Chômeur",
                        "En recherche d'emploi ou chômeur",
                        "Demandeur d'emploi"
                    )
                then "Chômeur, En recherche d'emploi"
                else user_activity
            end as user_activity,
            case
                when user_civility in ("M", "M.")
                then "M."
                when user_civility in ("Mme")
                then "Mme"
                else user_civility
            end as user_civility,
            user_school_type,
            user_is_active,
            user_age,
            user_role,
            user_birth_date,
        from {{ source("raw", "applicative_database_user") }} as u
        -- only BENEFICIARY
        where user_role in ('UNDERAGE_BENEFICIARY', 'BENEFICIARY')
    ),

    ranked_bookings as (
        select
            booking.user_id,
            offer.offer_subcategoryid,
            booking_used_date,
            rank() over (
                partition by booking.user_id
                order by booking.booking_creation_date asc, booking.booking_id asc
            ) as rank_
        from {{ source("raw", "applicative_database_booking") }} as booking
        join
            {{ source("raw", "applicative_database_stock") }} as stock
            on booking.stock_id = stock.stock_id
        join {{ ref("int_raw__offer") }} as offer on stock.offer_id = offer.offer_id
    )

select
    u.user_id,
    user_creation_date,
    user_humanized_id,
    user_has_enabled_marketing_email,
    -- set 99 when user user_creation_date does not match opening phases.
    -- this is due to Support changes in the past which migh lead to misunderstandings.
    case
        -- if user_department is not in "pilote" (2019_02 / 2019_06) phase but has
        -- created an account before, set 99.
        when
            u.user_department_code not in ("29", "34", "67", "93", "973")
            and date(user_creation_date) < "2019-06-01"
        then "99"
        -- if user_department is not in "experimentation" (2019_06 / 2021_05) phase
        -- but has created an account before, set 99.
        when
            u.user_department_code not in (
                "29",
                "34",
                "67",
                "93",
                "973",
                "22",
                "25",
                "35",
                "56",
                "58",
                "71",
                "08",
                "84",
                "94"
            )
            and date(user_creation_date) < "2021-05-01"
        then "99"
        else u.user_department_code
    end as user_department_code,
    u.user_postal_code,
    user_activity,
    user_civility,
    user_school_type,
    user_is_active,
    user_age,
    user_role,
    user_birth_date,
    case
        -- get user activation date with fictional offers (early 2019)
        when offer_subcategoryid = 'ACTIVATION_THING' and booking_used_date is not null
        then booking_used_date
        else user_creation_date
    end as user_activation_date,
    ui.user_iris_internal_id,
    ui.user_region_name,
    ui.user_city,
    ui.user_epci,
    ui.user_academy_name,
    ui.user_density_label,
    ui.user_macro_density_label,
    case when ui.qpv_name is not null then true else false end as user_is_in_qpv,
    case
        when u.user_activity = "Chômeur, En recherche d'emploi" then true else false
    end as user_is_unemployed,
    case
        when
            u.user_activity
            in ("Collégien", "Etudiant", "Lycéen", "Apprenti", "Alternant")
        then true
        else false
    end as user_is_in_education,
    case
        when
            (
                (ui.qpv_name is not null)
                or ui.user_macro_density_label = "rural"
                or u.user_activity
                not in ("Collégien", "Etudiant", "Lycéen", "Apprenti", "Alternant")
            )
        then true
        else false
    end as user_is_priority_public
from user_beneficiary as u
left join {{ ref("int_geo__user_location") }} as ui on ui.user_id = u.user_id
left join ranked_bookings on u.user_id = ranked_bookings.user_id and rank_ = 1
