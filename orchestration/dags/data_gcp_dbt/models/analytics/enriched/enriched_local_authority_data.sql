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
    mrt_global__offerer.offerer_id,
    REPLACE(mrt_global__offerer.partner_id, 'offerer', 'local-authority') as local_authority_id,
    mrt_global__offerer.offerer_name as local_authority_name,
    case
        when (
            LOWER(mrt_global__offerer.offerer_name) like 'commune%'
            or LOWER(mrt_global__offerer.offerer_name) like '%ville%de%'
        ) then 'Communes'
        when (
            LOWER(mrt_global__offerer.offerer_name) like '%departement%'
            or LOWER(mrt_global__offerer.offerer_name) like '%département%'
        ) then 'Départements'
        when (
            LOWER(mrt_global__offerer.offerer_name) like 'region%'
            or LOWER(mrt_global__offerer.offerer_name) like 'région%'
        ) then 'Régions'
        when (
            LOWER(mrt_global__offerer.offerer_name) like 'ca%'
            or LOWER(mrt_global__offerer.offerer_name) like '%agglo%'
            or LOWER(mrt_global__offerer.offerer_name) like 'cc%'
            or LOWER(mrt_global__offerer.offerer_name) like 'cu%'
            or LOWER(mrt_global__offerer.offerer_name) like '%communaute%'
            or LOWER(mrt_global__offerer.offerer_name) like '%agglomeration%'
            or LOWER(mrt_global__offerer.offerer_name) like '%agglomération%'
            or LOWER(mrt_global__offerer.offerer_name) like '%metropole%'
            or LOWER(mrt_global__offerer.offerer_name) like '%com%com%'
            or LOWER(mrt_global__offerer.offerer_name) like '%petr%'
            or LOWER(mrt_global__offerer.offerer_name) like '%intercommunal%'
        ) then 'CC / Agglomérations / Métropoles'
        else 'Non qualifiable'
    end as local_authority_type,
    case when mrt_global__offerer.offerer_id in (select priority_offerer_id from {{ source('seed','priority_local_authorities') }}) then TRUE else FALSE end as is_priority,
    COALESCE(applicative_database_offerer.offerer_validation_date, applicative_database_offerer.offerer_creation_date) as local_authority_creation_date,
    case when DATE_TRUNC(COALESCE(mrt_global__offerer.offerer_validation_date, mrt_global__offerer.offerer_creation_date), year) <= DATE_TRUNC(DATE_SUB(DATE(CURRENT_DATE/*'{{ ds }}'*/), interval 1 year), year) then TRUE else FALSE end as was_registered_last_year,
    mrt_global__offerer.academy_name as local_authority_academy_name,
    mrt_global__offerer.offerer_region_name as local_authority_region_name,
    mrt_global__offerer.offerer_department_code as local_authority_department_code,
    applicative_database_offerer.offerer_postal_code as local_authority_postal_code,
    case when DATE_DIFF(CURRENT_DATE, mrt_global__offerer.last_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, mrt_global__offerer.last_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_active_current_year,
    total_managed_venues,
    total_physical_managed_venues,
    total_permanent_managed_venues,
    case when nb_administrative_venues >= 1 then TRUE else FALSE end as has_administrative_venue,
    all_physical_venues_types,
    active_last_30days_physical_venues_types,
    top_bookings_venue.venue_type_label as top_bookings_venue_type,
    top_ca_venue.venue_type_label as top_ca_venue_type,
    nb_reimbursement_points,
    COALESCE(mrt_global__offerer.total_created_individual_offers, 0) as individual_offers_created,
    COALESCE(mrt_global__offerer.total_created_collective_offers, 0) as collective_offers_created,
    COALESCE(mrt_global__offerer.total_created_individual_offers, 0) + COALESCE(mrt_global__offerer.total_created_collective_offers, 0) as total_offers_created,
    mrt_global__offerer.first_offer_creation_date,
    mrt_global__offerer.last_bookable_offer_date,
    mrt_global__offerer.first_bookable_offer_date,
    COALESCE(mrt_global__offerer.total_non_cancelled_individual_bookings, 0) as non_cancelled_individual_bookings,
    COALESCE(mrt_global__offerer.total_used_individual_bookings, 0) as used_individual_bookings,
    COALESCE(mrt_global__offerer.total_non_cancelled_collective_bookings, 0) as confirmed_collective_bookings,
    COALESCE(mrt_global__offerer.total_used_collective_bookings, 0) as used_collective_bookings,
    COALESCE(mrt_global__offerer.total_individual_real_revenue, 0) as individual_real_revenue,
    COALESCE(mrt_global__offerer.total_collective_real_revenue, 0) as collective_real_revenue,
    COALESCE(mrt_global__offerer.total_real_revenue, 0) as total_real_revenue
from {{ ref('mrt_global__offerer') }} as mrt_global__offerer
    join {{ ref('offerer') }} as  applicative_database_offerer on mrt_global__offerer.offerer_id = applicative_database_offerer.offerer_id
    join {{ source('analytics','region_department') }} as region_department on mrt_global__offerer.offerer_department_code = region_department.num_dep
    left join {{ ref('mrt_global__venue') }} as mrt_global__venue on mrt_global__offerer.offerer_id = mrt_global__venue.venue_managing_offerer_id
    left join aggregated_venue_types on mrt_global__offerer.offerer_id = aggregated_venue_types.offerer_id
    left join active_venues_last_30days on mrt_global__offerer.offerer_id = active_venues_last_30days.offerer_id
    left join administrative_venues on mrt_global__offerer.offerer_id = administrative_venues.offerer_id
    left join top_ca_venue
        on mrt_global__offerer.offerer_id = top_ca_venue.offerer_id
            and ca_rank = 1
    left join top_bookings_venue
        on mrt_global__offerer.offerer_id = top_bookings_venue.offerer_id
            and bookings_rank = 1
    left join reimbursement_points on mrt_global__offerer.offerer_id = reimbursement_points.offerer_id
where is_local_authority is TRUE
