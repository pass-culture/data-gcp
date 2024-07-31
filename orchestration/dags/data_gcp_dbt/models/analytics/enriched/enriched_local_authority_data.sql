with active_venues_last_30days as (
    select
        venue_managing_offerer_id as offerer_id,
        STRING_AGG(distinct CONCAT(' ', case when venue_type_label != 'Offre numérique' then venue_type_label end)) as active_last_30days_physical_venues_types
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        left join {{ ref('bookable_venue_history') }} on mrt_global__venue.venue_id = bookable_venue_history.venue_id
    where DATE_DIFF(CURRENT_DATE, partition_date, day) <= 30
    group by 1
    order by 1
),

administrative_venues as (
    select
        venue_managing_offerer_id as offerer_id,
        COUNT(case when mrt_global__venue.venue_type_label = 'Lieu administratif' then venue_id else NULL end) as nb_administrative_venues
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
    group by 1
),

top_ca_venue as (
    select
        venue_managing_offerer_id as offerer_id,
        mrt_global__venue.venue_type_label,
        RANK() over (partition by venue_managing_offerer_id order by total_real_revenue desc) as ca_rank
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
    where total_real_revenue > 0
),

top_bookings_venue as (
    select
        venue_managing_offerer_id as offerer_id,
        mrt_global__venue.venue_type_label,
        RANK() over (partition by venue_managing_offerer_id order by total_used_bookings desc) as bookings_rank
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
    where total_used_bookings > 0
),

reimbursement_points as (
    select
        venue_managing_offerer_id as offerer_id,
        COUNT(distinct applicative_database_venue_reimbursement_point_link.venue_id) as nb_reimbursement_points
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        left join {{ ref('venue_reimbursement_point_link') }} as applicative_database_venue_reimbursement_point_link
            on mrt_global__venue.venue_id = applicative_database_venue_reimbursement_point_link.venue_id
    group by 1
),

aggregated_venue_types as (
    select
        venue_managing_offerer_id as offerer_id,
        STRING_AGG(distinct CONCAT(' ', case when mrt_global__venue.venue_type_label != 'Offre numérique' then mrt_global__venue.venue_type_label end)) as all_physical_venues_types
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
    group by 1
)

select distinct
    enriched_offerer_data.offerer_id,
    REPLACE(enriched_offerer_data.partner_id, 'offerer', 'local-authority') as local_authority_id,
    enriched_offerer_data.offerer_name as local_authority_name,
    case
        when (
            LOWER(enriched_offerer_data.offerer_name) like 'commune%'
            or LOWER(enriched_offerer_data.offerer_name) like '%ville%de%'
        ) then 'Communes'
        when (
            LOWER(enriched_offerer_data.offerer_name) like '%departement%'
            or LOWER(enriched_offerer_data.offerer_name) like '%département%'
        ) then 'Départements'
        when (
            LOWER(enriched_offerer_data.offerer_name) like 'region%'
            or LOWER(enriched_offerer_data.offerer_name) like 'région%'
        ) then 'Régions'
        when (
            LOWER(enriched_offerer_data.offerer_name) like 'ca%'
            or LOWER(enriched_offerer_data.offerer_name) like '%agglo%'
            or LOWER(enriched_offerer_data.offerer_name) like 'cc%'
            or LOWER(enriched_offerer_data.offerer_name) like 'cu%'
            or LOWER(enriched_offerer_data.offerer_name) like '%communaute%'
            or LOWER(enriched_offerer_data.offerer_name) like '%agglomeration%'
            or LOWER(enriched_offerer_data.offerer_name) like '%agglomération%'
            or LOWER(enriched_offerer_data.offerer_name) like '%metropole%'
            or LOWER(enriched_offerer_data.offerer_name) like '%com%com%'
            or LOWER(enriched_offerer_data.offerer_name) like '%petr%'
            or LOWER(enriched_offerer_data.offerer_name) like '%intercommunal%'
        ) then 'CC / Agglomérations / Métropoles'
        else 'Non qualifiable'
    end as local_authority_type,
    case when enriched_offerer_data.offerer_id in (select priority_offerer_id from {{ source('analytics','priority_local_authorities') }}) then TRUE else FALSE end as is_priority,
    COALESCE(applicative_database_offerer.offerer_validation_date, applicative_database_offerer.offerer_creation_date) as local_authority_creation_date,
    case when DATE_TRUNC(COALESCE(enriched_offerer_data.offerer_validation_date, enriched_offerer_data.offerer_creation_date), year) <= DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE/*'{{ ds }}'*/), interval 1 year), year) then TRUE else FALSE end as was_registered_last_year,
    academy_name as local_authority_academy_name,
    enriched_offerer_data.offerer_region_name as local_authority_region_name,
    enriched_offerer_data.offerer_department_code as local_authority_department_code,
    applicative_database_offerer.offerer_postal_code as local_authority_postal_code,
    case when DATE_DIFF(CURRENT_DATE, offerer_last_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, offerer_last_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_active_current_year,
    total_venues_managed,
    physical_venues_managed,
    permanent_venues_managed,
    case when nb_administrative_venues >= 1 then TRUE else FALSE end as has_administrative_venue,
    all_physical_venues_types,
    active_last_30days_physical_venues_types,
    top_bookings_venue.venue_type_label as top_bookings_venue_type,
    top_ca_venue.venue_type_label as top_ca_venue_type,
    nb_reimbursement_points,
    COALESCE(offerer_individual_offers_created, 0) as individual_offers_created,
    COALESCE(offerer_collective_offers_created, 0) as collective_offers_created,
    COALESCE(offerer_individual_offers_created, 0) + COALESCE(offerer_collective_offers_created, 0) as total_offers_created,
    offerer_first_offer_creation_date as first_offer_creation_date,
    offerer_last_bookable_offer_date as last_bookable_offer_date,
    offerer_first_bookable_offer_date as first_bookable_offer_date,
    COALESCE(offerer_non_cancelled_individual_bookings, 0) as non_cancelled_individual_bookings,
    COALESCE(offerer_used_individual_bookings, 0) as used_individual_bookings,
    COALESCE(offerer_non_cancelled_collective_bookings, 0) as confirmed_collective_bookings,
    COALESCE(offerer_used_collective_bookings, 0) as used_collective_bookings,
    COALESCE(offerer_individual_real_revenue, 0) as individual_real_revenue,
    COALESCE(offerer_collective_real_revenue, 0) as collective_real_revenue,
    COALESCE(offerer_real_revenue, 0) as total_real_revenue
from {{ ref('enriched_offerer_data') }}
    join {{ ref('offerer') }} as  applicative_database_offerer on enriched_offerer_data.offerer_id = applicative_database_offerer.offerer_id
    join {{ source('analytics','region_department') }} as region_department on enriched_offerer_data.offerer_department_code = region_department.num_dep
    left join {{ ref('mrt_global__venue') }} as mrt_global__venue on enriched_offerer_data.offerer_id = mrt_global__venue.venue_managing_offerer_id
    left join aggregated_venue_types on enriched_offerer_data.offerer_id = aggregated_venue_types.offerer_id
    left join active_venues_last_30days on enriched_offerer_data.offerer_id = active_venues_last_30days.offerer_id
    left join administrative_venues on enriched_offerer_data.offerer_id = administrative_venues.offerer_id
    left join top_ca_venue
        on enriched_offerer_data.offerer_id = top_ca_venue.offerer_id
            and ca_rank = 1
    left join top_bookings_venue
        on enriched_offerer_data.offerer_id = top_bookings_venue.offerer_id
            and bookings_rank = 1
    left join reimbursement_points on enriched_offerer_data.offerer_id = reimbursement_points.offerer_id
where is_local_authority is TRUE
