with main_venue_tag_per_offerer as (
    select
        venue_id,
        venue_managing_offerer_id,
        venue_tag_name as partner_type,
        "venue_tag" as partner_type_origin
    from {{ ref("mrt_global__venue_tag") }}
    where venue_tag_category_label = "Comptage partenaire sectoriel"
        and offerer_rank_desc = 1
),

main_venue_type_per_offerer as (
    select
        venue_id,
        venue_managing_offerer_id,
        venue_type_label as partner_type,
        "venue_type_label" as partner_type_origin
    from {{ ref("mrt_global__venue") }}
    where (total_created_offers > 0 or venue_type_label != "Offre numérique")
        and offerer_rank_asc = 1
),

top_venue_per_offerer as (
    select
        main_venue_type_per_offerer.venue_managing_offerer_id,
        COALESCE(main_venue_tag_per_offerer.venue_id, main_venue_type_per_offerer.venue_id) as venue_id,
        COALESCE(main_venue_tag_per_offerer.partner_type, main_venue_type_per_offerer.partner_type) as partner_type,
        COALESCE(main_venue_tag_per_offerer.partner_type_origin, main_venue_type_per_offerer.partner_type_origin) as partner_type_origin
    from main_venue_type_per_offerer
        left join main_venue_tag_per_offerer on main_venue_type_per_offerer.venue_managing_offerer_id = main_venue_tag_per_offerer.venue_managing_offerer_id
)

(
    select
        NULL as venue_id,
        o.offerer_id,
        o.partner_id,
        o.offerer_creation_date as partner_creation_date,
        case when DATE_TRUNC(offerer_creation_date, year) <= DATE_TRUNC(DATE_SUB(DATE("{{ ds() }}"), interval 1 year), year) then TRUE else NULL end as was_registered_last_year,
        o.offerer_name as partner_name,
        o.academy_name as partner_academy_name,
        o.offerer_region_name as partner_region_name,
        o.offerer_department_code as partner_department_code,
        o.offerer_postal_code as partner_postal_code,
        COALESCE(o.partner_type, top_venue_per_offerer.partner_type, "Structure non tagguée") as partner_type,
        case when o.partner_type is not NULL then "offerer_tag"
            when top_venue_per_offerer.partner_type_origin = "venue_tag" then "most_active_venue_tag"
            when top_venue_per_offerer.partner_type_origin = "venue_type_label" then "most_active_venue_type"
            else NULL
        end as partner_type_origin,
        agg_partner_cultural_sector.cultural_sector as cultural_sector,
        o.dms_accepted_at as dms_accepted_at,
        o.first_dms_adage_status as first_dms_adage_status,
        o.is_reference_adage as is_reference_adage,
        o.is_synchro_adage as is_synchro_adage,
        o.is_active_last_30days,
        o.is_active_current_year,
        o.is_individual_active_last_30days,
        o.is_individual_active_current_year,
        o.is_collective_active_last_30days,
        o.is_collective_active_current_year,
        o.total_created_individual_offers,
        o.total_created_collective_offers,
        o.total_created_offers,
        o.first_offer_creation_date,
        o.first_individual_offer_creation_date,
        o.first_collective_offer_creation_date,
        o.last_bookable_offer_date,
        o.first_bookable_offer_date,
        o.first_individual_bookable_offer_date,
        o.last_individual_bookable_offer_date,
        o.first_collective_bookable_offer_date,
        o.last_collective_bookable_offer_date,
        o.total_non_cancelled_individual_bookings,
        o.total_used_individual_bookings,
        o.total_non_cancelled_collective_bookings,
        o.total_used_collective_bookings,
        o.total_individual_real_revenue,
        o.total_collective_real_revenue,
        o.total_real_revenue,
        "offerer" as partner_status
    from {{ ref("mrt_global__offerer") }} as o
        left join {{ ref("mrt_global__venue") }} as v
            on v.offerer_id = o.offerer_id
                and v.venue_is_permanent
        left join top_venue_per_offerer on top_venue_per_offerer.venue_managing_offerer_id = o.offerer_id
        left join {{ source("seed", "agg_partner_cultural_sector") }} on agg_partner_cultural_sector.partner_type = COALESCE(o.partner_type, top_venue_per_offerer.partner_type)
    where not o.is_local_authority
        and v.offerer_id is NULL
)

union all

(
    select
        v.venue_id,
        v.venue_managing_offerer_id as offerer_id,
        v.partner_id,
        venue_creation_date as partner_creation_date,
        case when DATE_TRUNC(venue_creation_date, year) <= DATE_TRUNC(DATE_SUB(DATE("{{ ds() }}"), interval 1 year), year) then TRUE else FALSE end as was_registered_last_year,
        v.venue_name as partner_name,
        v.venue_academy_name as partner_academy_name,
        v.venue_region_name as partner_region_name,
        v.venue_department_code as partner_department_code,
        v.venue_postal_code as partner_postal_code,
        COALESCE(vt.venue_tag_name, v.venue_type_label) as partner_type,
        case when vt.venue_tag_name is not NULL then "venue_tag" else "venue_type_label" end as partner_type_origin,
        agg_partner_cultural_sector.cultural_sector as cultural_sector,
        v.dms_accepted_at as dms_accepted_at,
        v.first_dms_adage_status as first_dms_adage_status,
        v.is_reference_adage as is_reference_adage,
        v.is_synchro_adage as is_synchro_adage,
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
        "venue" as partner_status
    from {{ ref("mrt_global__venue") }} as v
        left join {{ source("seed", "agg_partner_cultural_sector") }} on agg_partner_cultural_sector.partner_type = v.venue_type_label
        left join {{ ref("mrt_global__venue_tag") }} as vt on v.venue_id = vt.venue_id and vt.venue_tag_category_label = "Comptage partenaire sectoriel"
    where venue_is_permanent
)
