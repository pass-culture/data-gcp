select
    v.venue_id,
    v.offerer_id,
    v.venue_id as partner_id,
    v.venue_creation_date as partner_creation_date,
    case
        when
            date_trunc(v.venue_creation_date, year)
            <= date_trunc(date_sub(date("{{ ds() }}"), interval 1 year), year)
        then true
        else false
    end as was_registered_last_year,
    v.venue_name as partner_name,
    v.venue_academy_name as partner_academy_name,
    v.venue_region_name as partner_region_name,
    v.venue_department_code as partner_department_code,
    v.venue_department_name as partner_department_name,
    v.venue_postal_code as partner_postal_code,
    v.venue_type_label as partner_type,
    "venue_type_label" as partner_type_origin,
    agg_partner_cultural_sector.cultural_sector,
    v.dms_accepted_at,
    v.first_dms_adage_status,
    v.is_reference_adage,
    v.is_synchro_adage,
    v.is_active_last_30days,
    v.is_active_current_year,
    v.is_individual_active_last_30days,
    v.is_individual_active_current_year,
    v.is_collective_active_last_30days,
    v.is_collective_active_current_year,
    v.total_created_individual_offers,
    v.total_created_collective_offers,
    v.total_created_offers,
    v.first_offer_creation_date,
    v.first_individual_offer_creation_date,
    v.first_collective_offer_creation_date,
    v.last_bookable_offer_date,
    v.first_bookable_offer_date,
    v.first_individual_bookable_offer_date,
    v.last_individual_bookable_offer_date,
    v.first_collective_bookable_offer_date,
    v.last_collective_bookable_offer_date,
    v.total_non_cancelled_individual_bookings,
    v.total_used_individual_bookings,
    v.total_non_cancelled_collective_bookings,
    v.total_used_collective_bookings,
    v.total_individual_real_revenue,
    v.total_collective_real_revenue,
    v.total_real_revenue,
    case when v.venue_is_open_to_public then "ERP" else "non ERP" end as partner_status
from {{ ref("mrt_global__venue") }} as v
left join
    {{ source("seed", "agg_partner_cultural_sector") }}
    on v.venue_type_label = agg_partner_cultural_sector.partner_type
where v.venue_is_open_to_public or v.venue_siret is not null or v.venue_is_permanent
