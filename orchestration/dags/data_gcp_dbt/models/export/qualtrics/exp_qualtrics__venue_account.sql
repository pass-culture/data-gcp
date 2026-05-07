with
    previous_export as (
        select distinct venue_booking_email
        from {{ source("raw", "qualtrics_exported_venue_account") }}
        where
            calculation_month
            >= date_sub(date_trunc(date("{{ ds() }}"), month), interval 3 month)
    ),

    venues as (
        select
            global_venue.venue_id,
            global_venue.venue_booking_email,
            global_venue.venue_name,
            global_venue.venue_type_label,
            global_venue.venue_region_name,
            global_venue.venue_department_code,
            global_venue.venue_rural_city_type,
            global_venue.total_real_revenue,
            global_venue.total_non_cancelled_individual_bookings,
            global_venue.total_non_cancelled_collective_bookings,
            global_venue.is_active_current_year,
            global_venue.is_individual_active_current_year,
            global_venue.is_collective_active_current_year,
            date_diff(
                current_date(), global_venue.venue_creation_date, day
            ) as venue_seniority_days
        from {{ ref("mrt_global__venue") }} as global_venue
        left join
            {{ source("raw", "qualtrics_opt_out_users") }} as opt_out
            on global_venue.venue_id = opt_out.ext_ref
        where
            opt_out.contact_id is null and global_venue.venue_booking_email is not null
    ),

    generate_export as (
        select v.*
        from venues as v
        left join
            previous_export as pe on v.venue_booking_email = pe.venue_booking_email
        where pe.venue_booking_email is null
        order by rand()
        limit 4000
    )

select *, current_date as export_date
from generate_export
