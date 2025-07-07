with
    previous_export as (
        select distinct email
        from {{ source("raw", "qualtrics_exported_venue_account") }}
        where
            calculation_month
            >= date_sub(date_trunc(date("{{ ds() }}"), month), interval 6 month)
    ),

    answers as (select distinct user_id from {{ source("raw", "qualtrics_answers") }}),

    lieux_physique as (
        select
            global_venue.venue_id,
            global_venue.venue_booking_email as email,
            global_venue.venue_type_label,
            global_venue.total_non_cancelled_bookings,
            global_venue.total_created_individual_offers,
            global_venue.total_created_collective_offers,
            global_venue.venue_is_permanent,
            global_venue.venue_region_name,
            global_venue.venue_department_code,
            global_venue.venue_rural_city_type as geo_type,
            global_venue.venue_in_qpv,
            global_venue.venue_in_zrr,
            date_diff(
                current_date(), global_venue.venue_creation_date, day
            ) as seniority_day_cnt,
            global_venue.total_created_individual_offers
            + global_venue.total_created_collective_offers as offers_created
        from {{ ref("mrt_global__venue") }} as global_venue
        left join
            {{ source("raw", "qualtrics_opt_out_users") }} as opt_out
            on global_venue.venue_id = opt_out.ext_ref
        left join answers on global_venue.venue_id = answers.user_id
        where
            not global_venue.venue_is_virtual
            and opt_out.contact_id is null
            and answers.user_id is null
    ),

    generate_export as (
        select lp.*
        from lieux_physique as lp
        left join previous_export as pe on lp.email = pe.email
        where pe.email is null
        order by rand()
        limit 3500
    )

select current_date as export_date, *
from generate_export
