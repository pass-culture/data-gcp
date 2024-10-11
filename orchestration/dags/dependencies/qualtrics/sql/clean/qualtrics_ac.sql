with
    previous_export as (
        select distinct email
        from `{{ bigquery_clean_dataset }}.qualtrics_ac`
        where
            calculation_month
            >= date_sub(date("{{ current_month(ds) }}"), interval 6 month)
    ),

    answers as (
        select distinct user_id from `{{ bigquery_raw_dataset }}.qualtrics_answers`
    ),

    lieux_physique as (
        select
            global_venue.venue_id,
            venue_booking_email as email,
            venue_type_label,
            date_diff(current_date, venue_creation_date, day) as anciennete_en_jours,
            total_non_cancelled_bookings,
            total_created_individual_offers,
            total_created_collective_offers,
            total_created_individual_offers
            + total_created_collective_offers as offers_created,
            venue_is_permanent,
            global_venue.venue_region_name,
            -- TODO rename field in qualtrics
            global_venue.venue_department_code,
            venue_location.venue_rural_city_type as geo_type,
            venue_location.venue_in_qpv,
            venue_location.venue_in_zrr
        from `{{ bigquery_analytics_dataset }}.global_venue` global_venue
        left join
            `{{ bigquery_int_geo_dataset }}.venue_location` venue_location
            on venue_location.venue_id = global_venue.venue_id
        left join
            `{{ bigquery_raw_dataset }}.qualtrics_opt_out_users` opt_out
            on opt_out.ext_ref = global_venue.venue_id
        left join answers on global_venue.venue_id = answers.user_id
        where
            not venue_is_virtual
            and opt_out.contact_id is null
            and answers.user_id is null
    ),

    generate_export as (
        select lp.*
        from lieux_physique lp
        left join previous_export pe on pe.email = lp.email
        where pe.email is null
        order by rand()
        limit {{ params.volume }}
    )

select
    date("{{ current_month(ds) }}") as calculation_month, current_date as export_date, *
from generate_export
