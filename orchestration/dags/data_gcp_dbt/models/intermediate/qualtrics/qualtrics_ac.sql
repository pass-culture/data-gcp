with
    previous_export as (
        select distinct email
        from {{ source("clean", "qualtrics_ac") }}
        where
            calculation_month
            >= date_sub(date_trunc(date("{{ ds() }}"), month), interval 6 month)
    ),

    answers as (select distinct user_id from {{ source("raw", "qualtrics_answers") }}),

    lieux_physique as (
        select
            global_venue.venue_id,
            venue_booking_email as email,
            venue_type_label,
            total_non_cancelled_bookings,
            total_created_individual_offers,
            total_created_collective_offers,
            venue_is_permanent,
            global_venue.venue_region_name,
            global_venue.venue_department_code,
            venue_location.venue_rural_city_type as geo_type,
            -- TODO rename field in qualtrics
            venue_location.venue_in_qpv,
            venue_location.venue_in_zrr,
            date_diff(current_date, venue_creation_date, day) as anciennete_en_jours,
            total_created_individual_offers
            + total_created_collective_offers as offers_created
        from {{ ref("mrt_global__venue") }} as global_venue
        left join
            {{ ref("int_geo__venue_location") }} as venue_location
            on global_venue.venue_id = venue_location.venue_id
        left join
            {{ source("raw", "qualtrics_opt_out_users") }} as opt_out
            on global_venue.venue_id = opt_out.ext_ref
        left join answers on global_venue.venue_id = answers.user_id
        where
            not venue_is_virtual
            and opt_out.contact_id is null
            and answers.user_id is null
    ),

    generate_export as (
        select lp.*
        from lieux_physique as lp
        left join previous_export as pe on lp.email = pe.email
        where pe.email is null
        order by rand()
        limit {{ qualtrics_volumes() }}
    )

select
    date_trunc(date("{{ ds() }}"), month) as calculation_month,
    current_date as export_date,
    *
from generate_export
