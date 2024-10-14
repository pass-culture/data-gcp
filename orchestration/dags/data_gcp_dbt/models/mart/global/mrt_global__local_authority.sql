with
    active_venues_last_30days as (
        select
            venue_managing_offerer_id as offerer_id,
            string_agg(
                distinct concat(
                    " ",
                    case
                        when venue_type_label != "Offre numérique" then venue_type_label
                    end
                )
            ) as active_last_30days_physical_venues_types
        from {{ ref("mrt_global__venue") }} as v
        left join
            {{ ref("bookable_venue_history") }}
            on v.venue_id = bookable_venue_history.venue_id
        where date_diff(date("{{ ds() }}"), partition_date, day) <= 30
        group by offerer_id
    )

select distinct
    ofr.offerer_id,
    replace(ofr.partner_id, "offerer", "local-authority") as local_authority_id,
    ofr.offerer_name as local_authority_name,
    case
        when
            (
                lower(ofr.offerer_name) like "commune%"
                or lower(ofr.offerer_name) like "%ville%de%"
            )
        then "Communes"
        when
            (
                lower(ofr.offerer_name) like "%departement%"
                or lower(ofr.offerer_name) like "%département%"
            )
        then "Départements"
        when
            (
                lower(ofr.offerer_name) like "region%"
                or lower(ofr.offerer_name) like "région%"
            )
        then "Régions"
        when
            (
                lower(ofr.offerer_name) like "ca%"
                or lower(ofr.offerer_name) like "%agglo%"
                or lower(ofr.offerer_name) like "cc%"
                or lower(ofr.offerer_name) like "cu%"
                or lower(ofr.offerer_name) like "%communaute%"
                or lower(ofr.offerer_name) like "%agglomeration%"
                or lower(ofr.offerer_name) like "%agglomération%"
                or lower(ofr.offerer_name) like "%metropole%"
                or lower(ofr.offerer_name) like "%com%com%"
                or lower(ofr.offerer_name) like "%petr%"
                or lower(ofr.offerer_name) like "%intercommunal%"
            )
        then "CC / Agglomérations / Métropoles"
        else "Non qualifiable"
    end as local_authority_type,
    case
        when
            ofr.offerer_id in (
                select priority_offerer_id
                from {{ source("seed", "priority_local_authorities") }}
            )
        then true
        else false
    end as is_priority,
    coalesce(
        ofr.offerer_validation_date, ofr.offerer_creation_date
    ) as local_authority_creation_date,
    case
        when
            date_trunc(
                coalesce(ofr.offerer_validation_date, ofr.offerer_creation_date),
                year
            ) <= date_trunc(
                date_sub(
                    date(
                        current_date  /* ""{{ ds }}""*/
                    ),
                    interval 1 year
                ),
                year
            )
        then true
        else false
    end as was_registered_last_year,
    ofr.academy_name as local_authority_academy_name,
    ofr.offerer_region_name as local_authority_region_name,
    ofr.offerer_department_code as local_authority_department_code,
    ofr.offerer_postal_code as local_authority_postal_code,
    ofr.is_active_last_30days,
    ofr.is_active_current_year,
    ofr.total_managed_venues,
    ofr.total_physical_managed_venues,
    ofr.total_permanent_managed_venues,
    case
        when ofr.total_administrative_venues >= 1 then true else false
    end as has_administrative_venue,
    ofr.all_physical_venues_types,
    active_last_30days_physical_venues_types,
    ofr.top_bookings_venue_type,
    ofr.top_real_revenue_venue_type,
    ofr.total_reimbursement_points,
    ofr.total_created_individual_offers,
    ofr.total_created_collective_offers,
    ofr.total_created_offers,
    ofr.first_offer_creation_date,
    ofr.last_bookable_offer_date,
    ofr.first_bookable_offer_date,
    ofr.total_non_cancelled_individual_bookings,
    ofr.total_used_individual_bookings,
    ofr.total_non_cancelled_collective_bookings,
    ofr.total_used_collective_bookings,
    ofr.total_individual_real_revenue,
    ofr.total_collective_real_revenue,
    ofr.total_real_revenue
from {{ ref("int_global__offerer") }} as ofr
left join
    active_venues_last_30days on ofr.offerer_id = active_venues_last_30days.offerer_id
where
    ofr.is_local_authority is true
    and ofr.offerer_validation_status = "VALIDATED"
    and ofr.offerer_is_active
