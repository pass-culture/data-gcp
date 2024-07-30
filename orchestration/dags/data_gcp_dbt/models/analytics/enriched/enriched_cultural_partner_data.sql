with permanent_venues as (
    select
        mrt_global__venue.venue_id as venue_id,
        mrt_global__venue.venue_managing_offerer_id as offerer_id,
        mrt_global__venue.partner_id,
        venue_creation_date as partner_creation_date,
        case when DATE_TRUNC(venue_creation_date, year) <= DATE_TRUNC(DATE_SUB(DATE('{{ ds() }}'), interval 1 year), year) then TRUE else FALSE end as was_registered_last_year,
        mrt_global__venue.venue_name as partner_name,
        region_department.academy_name as partner_academy_name,
        mrt_global__venue.venue_region_name as partner_region_name,
        mrt_global__venue.venue_department_code as partner_department_code,
        mrt_global__venue.venue_postal_code as partner_postal_code,
        'venue' as partner_status,
        COALESCE(venue_tag_name, venue_type_label) as partner_type,
        case
            when
                venue_tag_name is not NULL then "venue_tag"
            else 'venue_type_label'
        end
            as partner_type_origin,
        agg_partner_cultural_sector.cultural_sector as cultural_sector,
        enriched_offerer_data.dms_accepted_at as dms_accepted_at,
        enriched_offerer_data.first_dms_adage_status as first_dms_adage_status,
        enriched_offerer_data.is_reference_adage as is_reference_adage,
        enriched_offerer_data.is_synchro_adage as is_synchro_adage,
        case when DATE_DIFF(CURRENT_DATE, last_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_active_last_30days,
        case when DATE_DIFF(CURRENT_DATE, last_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_active_current_year,
        case when DATE_DIFF(CURRENT_DATE, last_individual_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_individual_active_last_30days,
        case when DATE_DIFF(CURRENT_DATE, last_individual_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_individual_active_current_year,
        case when DATE_DIFF(CURRENT_DATE, last_collective_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_collective_active_last_30days,
        case when DATE_DIFF(CURRENT_DATE, last_collective_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_collective_active_current_year,
        COALESCE(mrt_global__venue.total_created_individual_offers, 0) as individual_offers_created,
        COALESCE(mrt_global__venue.total_created_collective_offers, 0) as collective_offers_created,
        (COALESCE(mrt_global__venue.total_created_collective_offers, 0) + COALESCE(mrt_global__venue.total_created_individual_offers, 0)) as total_offers_created,
        mrt_global__venue.first_offer_creation_date as first_offer_creation_date,
        mrt_global__venue.first_individual_offer_creation_date as first_individual_offer_creation_date,
        mrt_global__venue.first_collective_offer_creation_date as first_collective_offer_creation_date,
        last_bookable_offer_date,
        first_bookable_offer_date,
        first_individual_bookable_offer_date,
        last_individual_bookable_offer_date,
        first_collective_bookable_offer_date,
        last_collective_bookable_offer_date,
        COALESCE(mrt_global__venue.total_non_cancelled_individual_bookings, 0) as non_cancelled_individual_bookings,
        COALESCE(mrt_global__venue.total_used_individual_bookings, 0) as used_individual_bookings,
        COALESCE(mrt_global__venue.total_non_cancelled_collective_bookings, 0) as confirmed_collective_bookings,
        COALESCE(mrt_global__venue.total_used_collective_bookings, 0) as used_collective_bookings,
        COALESCE(mrt_global__venue.total_individual_real_revenue, 0) as real_individual_revenue,
        COALESCE(mrt_global__venue.total_collective_real_revenue, 0) as real_collective_revenue,
        (COALESCE(mrt_global__venue.total_individual_real_revenue, 0) + COALESCE(mrt_global__venue.total_collective_real_revenue, 0)) as total_real_revenue
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        left join {{ source('analytics', 'region_department') }} as region_department
            on mrt_global__venue.venue_department_code = region_department.num_dep
        left join {{ source('raw', 'agg_partner_cultural_sector') }} on agg_partner_cultural_sector.partner_type = mrt_global__venue.venue_type_label
        left join {{ ref('mrt_global__venue_tag') }} as mrt_global__venue_tag on mrt_global__venue.venue_id = mrt_global__venue_tag.venue_id and mrt_global__venue_tag.venue_tag_category_label = "Comptage partenaire sectoriel"
        left join {{ ref('enriched_offerer_data') }} as enriched_offerer_data
            on mrt_global__venue.venue_managing_offerer_id = enriched_offerer_data.offerer_id
    where venue_is_permanent is TRUE
),


tagged_partners as (
    select
        offerer_id,
        STRING_AGG(distinct case when tag_label is not NULL then tag_label else NULL end order by (case when tag_label is not NULL then tag_label else NULL end)) as partner_type
    from {{ ref('mrt_global__offerer_tag') }}
    where
        tag_category_name = 'comptage'
        and tag_label not in ('Association', 'EPN', 'Collectivité', 'Pas de tag associé', 'Auto-Entrepreneur', 'Compagnie', 'Tourneur')
    group by 1
),

-- On récupère tous les lieux taggués et on remonte le tag du lieu le + actif de chaque structure
top_venue_tag_per_offerer as (
    select
        mrt_global__venue.venue_id,
        mrt_global__venue.venue_managing_offerer_id as offerer_id,
        venue_tag_name as partner_type,
        'venue_tag' as partner_type_origin
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        join {{ ref('mrt_global__venue_tag') }}
            as mrt_global__venue_tag on mrt_global__venue.venue_id = mrt_global__venue_tag.venue_id
        and mrt_global__venue_tag.venue_tag_category_label = "Comptage partenaire sectoriel"
    qualify ROW_NUMBER() over (
        partition by mrt_global__venue.venue_managing_offerer_id
        order by
            total_theoretic_revenue desc,
            (COALESCE(mrt_global__venue.total_created_individual_offers, 0) + COALESCE(mrt_global__venue.total_created_collective_offers, 0)) desc,
            venue_name
    ) = 1
),

-- On récupère le label du lieu le + actif de chaque structure
top_venue_type_per_offerer as (
    select
        mrt_global__venue.venue_id,
        venue_managing_offerer_id as offerer_id,
        venue_type_label as partner_type,
        'venue_type_label'
            as partner_type_origin
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
    where (total_created_offers > 0 or venue_type_label != 'Offre numérique')
    qualify ROW_NUMBER() over (
        partition by venue_managing_offerer_id
        order by
            total_theoretic_revenue desc,
            (COALESCE(mrt_global__venue.total_created_individual_offers, 0) + COALESCE(mrt_global__venue.total_created_collective_offers, 0)) desc,
            venue_name asc
    ) = 1
),

top_venue_per_offerer as (
    select
        top_venue_type_per_offerer.offerer_id,
        COALESCE(top_venue_tag_per_offerer.venue_id, top_venue_type_per_offerer.venue_id) venue_id,
        COALESCE(top_venue_tag_per_offerer.partner_type, top_venue_type_per_offerer.partner_type) partner_type,
        COALESCE(top_venue_tag_per_offerer.partner_type_origin, top_venue_type_per_offerer.partner_type_origin) partner_type_origin
    from top_venue_type_per_offerer
        left join top_venue_tag_per_offerer on top_venue_type_per_offerer.offerer_id = top_venue_tag_per_offerer.offerer_id
),


offerers as (
    select
        '' as venue_id,
        enriched_offerer_data.offerer_id,
        enriched_offerer_data.partner_id,
        enriched_offerer_data.offerer_creation_date as partner_creation_date,
        case when DATE_TRUNC(enriched_offerer_data.offerer_creation_date, year) <= DATE_TRUNC(DATE_SUB(DATE('{{ ds() }}'), interval 1 year), year) then TRUE else NULL end as was_registered_last_year,
        enriched_offerer_data.offerer_name as partner_name,
        region_department.academy_name as partner_academy_name,
        enriched_offerer_data.offerer_region_name as partner_region_name,
        enriched_offerer_data.offerer_department_code as partner_department_code,
        applicative_database_offerer.offerer_postal_code as partner_postal_code,
        'offerer' as partner_status,
        COALESCE(tagged_partners.partner_type, top_venue_per_offerer.partner_type, 'Structure non tagguée') as partner_type,
        case
            when tagged_partners.partner_type is not NULL then 'offerer_tag'
            when top_venue_per_offerer.partner_type_origin = "venue_tag" then 'most_active_venue_tag'
            when top_venue_per_offerer.partner_type_origin = "venue_type_label" then "most_active_venue_type"
            else NULL
        end as partner_type_origin,
        agg_partner_cultural_sector.cultural_sector as cultural_sector,
        enriched_offerer_data.dms_accepted_at as dms_accepted_at,
        enriched_offerer_data.first_dms_adage_status as first_dms_adage_status,
        enriched_offerer_data.is_reference_adage as is_reference_adage,
        enriched_offerer_data.is_synchro_adage as is_synchro_adage,
        case when DATE_DIFF(CURRENT_DATE, enriched_offerer_data.offerer_last_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_active_last_30days,
        case when DATE_DIFF(CURRENT_DATE, enriched_offerer_data.offerer_last_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_active_current_year,
        case when DATE_DIFF(CURRENT_DATE, offerer_last_individual_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_individual_active_last_30days,
        case when DATE_DIFF(CURRENT_DATE, offerer_last_individual_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_individual_active_current_year,
        case when DATE_DIFF(CURRENT_DATE, offerer_last_collective_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_collective_active_last_30days,
        case when DATE_DIFF(CURRENT_DATE, offerer_last_collective_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_collective_active_current_year,
        COALESCE(enriched_offerer_data.offerer_individual_offers_created, 0) as individual_offers_created,
        COALESCE(enriched_offerer_data.offerer_collective_offers_created, 0) as collective_offers_created,
        COALESCE(enriched_offerer_data.offerer_individual_offers_created, 0) + COALESCE(enriched_offerer_data.offerer_collective_offers_created, 0) as total_offers_created,
        enriched_offerer_data.offerer_first_offer_creation_date as first_offer_creation_date,
        enriched_offerer_data.offerer_first_individual_offer_creation_date as first_individual_offer_creation_date,
        enriched_offerer_data.offerer_first_collective_offer_creation_date as first_collective_offer_creation_date,
        enriched_offerer_data.offerer_last_bookable_offer_date as last_bookable_offer_date,
        enriched_offerer_data.offerer_first_bookable_offer_date as first_bookable_offer_date,
        enriched_offerer_data.offerer_first_individual_bookable_offer_date as first_individual_bookable_offer_date,
        enriched_offerer_data.offerer_last_individual_bookable_offer_date as last_individual_bookable_offer_date,
        enriched_offerer_data.offerer_first_collective_bookable_offer_date as first_collective_bookable_offer_date,
        enriched_offerer_data.offerer_last_collective_bookable_offer_date as last_collective_bookable_offer_date,
        COALESCE(enriched_offerer_data.offerer_non_cancelled_individual_bookings, 0) as non_cancelled_individual_bookings,
        COALESCE(enriched_offerer_data.offerer_used_individual_bookings, 0) as used_individual_bookings,
        COALESCE(enriched_offerer_data.offerer_non_cancelled_collective_bookings, 0) as confirmed_collective_bookings,
        COALESCE(enriched_offerer_data.offerer_used_collective_bookings, 0) as used_collective_bookings,
        COALESCE(enriched_offerer_data.offerer_individual_real_revenue, 0) as real_individual_revenue,
        COALESCE(enriched_offerer_data.offerer_collective_real_revenue, 0) as real_collective_revenue,
        COALESCE(enriched_offerer_data.offerer_individual_real_revenue, 0) + COALESCE(enriched_offerer_data.offerer_collective_real_revenue, 0) as total_real_revenue
    from {{ ref('enriched_offerer_data') }}
        left join {{ source('raw', 'applicative_database_offerer') }} as applicative_database_offerer
            on enriched_offerer_data.offerer_id = applicative_database_offerer.offerer_id
        left join {{ source('analytics', 'region_department') }} as region_department
            on enriched_offerer_data.offerer_department_code = region_department.num_dep
        left join tagged_partners on tagged_partners.offerer_id = enriched_offerer_data.offerer_id
        left join permanent_venues on permanent_venues.offerer_id = enriched_offerer_data.offerer_id
        left join top_venue_per_offerer on top_venue_per_offerer.offerer_id = enriched_offerer_data.offerer_id
        left join {{ source('raw', 'agg_partner_cultural_sector') }} on agg_partner_cultural_sector.partner_type = COALESCE(tagged_partners.partner_type, top_venue_per_offerer.partner_type)
    where
        not enriched_offerer_data.is_local_authority  -- Collectivités à part
        and permanent_venues.offerer_id is NULL -- Pas déjà compté à l'échelle du lieu permanent
)

select
    venue_id,
    offerer_id,
    partner_id,
    partner_creation_date,
    was_registered_last_year,
    partner_name,
    partner_academy_name,
    partner_region_name,
    partner_department_code,
    partner_postal_code,
    partner_status,
    partner_type,
    partner_type_origin,
    cultural_sector,
    is_active_last_30days,
    is_active_current_year,
    is_individual_active_last_30days,
    is_individual_active_current_year,
    is_collective_active_last_30days,
    is_collective_active_current_year,
    individual_offers_created,
    collective_offers_created,
    total_offers_created,
    first_offer_creation_date,
    first_individual_offer_creation_date,
    first_collective_offer_creation_date,
    last_bookable_offer_date,
    first_bookable_offer_date,
    first_individual_bookable_offer_date,
    last_individual_bookable_offer_date,
    first_collective_bookable_offer_date,
    last_collective_bookable_offer_date,
    non_cancelled_individual_bookings,
    used_individual_bookings,
    confirmed_collective_bookings,
    used_collective_bookings,
    real_individual_revenue,
    real_collective_revenue,
    total_real_revenue
from permanent_venues
union all
select
    venue_id,
    offerer_id,
    partner_id,
    partner_creation_date,
    was_registered_last_year,
    partner_name,
    partner_academy_name,
    partner_region_name,
    partner_department_code,
    partner_postal_code,
    partner_status,
    partner_type,
    partner_type_origin,
    cultural_sector,
    is_active_last_30days,
    is_active_current_year,
    is_individual_active_last_30days,
    is_individual_active_current_year,
    is_collective_active_last_30days,
    is_collective_active_current_year,
    individual_offers_created,
    collective_offers_created,
    total_offers_created,
    first_offer_creation_date,
    first_individual_offer_creation_date,
    first_collective_offer_creation_date,
    last_bookable_offer_date,
    first_bookable_offer_date,
    first_individual_bookable_offer_date,
    last_individual_bookable_offer_date,
    first_collective_bookable_offer_date,
    last_collective_bookable_offer_date,
    non_cancelled_individual_bookings,
    used_individual_bookings,
    confirmed_collective_bookings,
    used_collective_bookings,
    real_individual_revenue,
    real_collective_revenue,
    total_real_revenue
from offerers
