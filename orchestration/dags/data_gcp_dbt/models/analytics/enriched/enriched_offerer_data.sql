{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

with offerer_humanized_id as (
    select
        offerer_id,
        {{ target_schema }}.humanize_id(offerer_id) as humanized_id
    from
        {{ source('raw', 'applicative_database_offerer') }}
    where
        offerer_id is not NULL
),

individual_bookings_per_offerer as (
    select
        venue.venue_managing_offerer_id as offerer_id,
        count(booking.booking_id) as total_individual_bookings,
        count(case when not booking.booking_is_cancelled then booking.booking_id else NULL end) as non_cancelled_individual_bookings,
        count(case when booking.booking_is_used then booking.booking_id else NULL end) as used_individual_bookings,
        coalesce(sum(case when not booking.booking_is_cancelled then booking.booking_intermediary_amount else NULL end), 0) as individual_theoretic_revenue,
        coalesce(sum(case when booking.booking_is_used then booking.booking_intermediary_amount else NULL end), 0) as individual_real_revenue,
        coalesce(sum(case when booking.booking_is_used and extract(year from booking.booking_creation_date) = extract(year from current_date) then booking.booking_intermediary_amount else NULL end), 0) as individual_current_year_real_revenue,
        min(booking.booking_creation_date) as first_individual_booking_date,
        max(booking.booking_creation_date) as last_individual_booking_date
    from
        {{ ref('venue') }} as venue
        left join {{ ref('offer') }} as offer on venue.venue_id = offer.venue_id
        left join {{ source('raw', 'applicative_database_stock') }} as stock on stock.offer_id = offer.offer_id
        left join {{ ref('booking') }} as booking on stock.stock_id = booking.stock_id
    group by
        venue.venue_managing_offerer_id
),

collective_bookings_per_offerer as (
    select
        collective_booking.offerer_id,
        count(collective_booking.collective_booking_id) as total_collective_bookings,
        count(case when collective_booking_status not in ('CANCELLED') then collective_booking.collective_booking_id else NULL end) as non_cancelled_collective_bookings,
        count(case when collective_booking_status in ('USED', 'REIMBURSED') then collective_booking.collective_booking_id else NULL end) as used_collective_bookings,
        coalesce(sum(case when collective_booking_status not in ('CANCELLED') then collective_stock.collective_stock_price else NULL end), 0) as collective_theoretic_revenue,
        coalesce(sum(case when collective_booking_status in ('USED', 'REIMBURSED') then collective_stock.collective_stock_price else NULL end), 0) as collective_real_revenue,
        coalesce(sum(case when collective_booking_status in ('USED', 'REIMBURSED') and extract(year from collective_booking.collective_booking_creation_date) = extract(year from current_date) then collective_stock.collective_stock_price else NULL end), 0) as collective_current_year_real_revenue,
        min(collective_booking.collective_booking_creation_date) as first_collective_booking_date,
        max(collective_booking.collective_booking_creation_date) as last_collective_booking_date
    from
        {{ source('raw', 'applicative_database_collective_booking') }} as collective_booking
        inner join {{ source('raw', 'applicative_database_collective_stock') }} as collective_stock on collective_stock.collective_stock_id = collective_booking.collective_stock_id
    group by
        collective_booking.offerer_id
),

individual_offers_per_offerer as (
    select
        venue.venue_managing_offerer_id as offerer_id,
        min(offer.offer_creation_date) as first_individual_offer_creation_date,
        max(offer.offer_creation_date) as last_individual_offer_creation_date,
        count(offer.offer_id) as individual_offers_created
    from
        {{ ref('venue') }} as venue
        left join {{ ref('offer') }} as offer on venue.venue_id = offer.venue_id and offer.offer_validation = 'APPROVED'
    group by
        venue.venue_managing_offerer_id
),

all_collective_offers as (
    select
        collective_offer_id,
        venue.venue_id,
        venue.venue_managing_offerer_id as offerer_id,
        collective_offer_creation_date
    from
        {{ source('raw', 'applicative_database_collective_offer') }} as collective_offer
        join {{ ref('venue') }} as venue on venue.venue_id = collective_offer.venue_id and collective_offer.collective_offer_validation = 'APPROVED'
    union
    all
    select
        collective_offer_id,
        venue.venue_id,
        venue.venue_managing_offerer_id as offerer_id,
        collective_offer_creation_date
    from
        {{ source('raw', 'applicative_database_collective_offer_template') }} as collective_offer_template
        join {{ ref('venue') }} as venue on venue.venue_id = collective_offer_template.venue_id and collective_offer_template.collective_offer_validation = 'APPROVED'

),

collective_offers_per_offerer as (
    select
        offerer_id,
        count(collective_offer_id) as collective_offers_created,
        min(collective_offer_creation_date) as first_collective_offer_creation_date,
        max(collective_offer_creation_date) as last_collective_offer_creation_date
    from
        all_collective_offers
    group by
        offerer_id
),

bookable_individual_offer_cnt as (
    select
        offerer_id,
        count(distinct offer_id) as offerer_bookable_individual_offer_cnt
    from
        {{ ref('bookable_offer') }}
    group by
        1
),

bookable_collective_offer_cnt as (
    select
        offerer_id,
        count(distinct collective_offer_id) as offerer_bookable_collective_offer_cnt
    from
        {{ ref('bookable_collective_offer') }}
    group by
        1
),

bookable_offer_history as (
    select
        offerer_id,
        min(partition_date) as offerer_first_bookable_offer_date,
        max(partition_date) as offerer_last_bookable_offer_date,
        min(case when individual_bookable_offers > 0 then partition_date else NULL end) as offerer_first_individual_bookable_offer_date,
        max(case when individual_bookable_offers > 0 then partition_date else NULL end) as offerer_last_individual_bookable_offer_date,
        min(case when collective_bookable_offers > 0 then partition_date else NULL end) as offerer_first_collective_bookable_offer_date,
        max(case when collective_bookable_offers > 0 then partition_date else NULL end) as offerer_last_collective_bookable_offer_date
    from {{ ref('bookable_venue_history') }}
    group by 1
),

related_stocks as (
    select
        offerer.offerer_id,
        min(stock.stock_creation_date) as first_stock_creation_date
    from
        {{ source('raw', 'applicative_database_offerer') }} as offerer
        left join {{ ref('venue') }} as venue on venue.venue_managing_offerer_id = offerer.offerer_id
        left join {{ ref('offer') }} as offer on offer.venue_id = venue.venue_id
        left join {{ source('raw', 'applicative_database_stock') }} as stock on stock.offer_id = offer.offer_id
    group by
        offerer_id
),

offerer_department_code as (
    select
        offerer.offerer_id,
        case
            when offerer_postal_code = '97150' then '978'
            when substring(offerer_postal_code, 0, 2) = '97' then substring(offerer_postal_code, 0, 3)
            when substring(offerer_postal_code, 0, 2) = '98' then substring(offerer_postal_code, 0, 3)
            when substring(offerer_postal_code, 0, 3) in ('200', '201', '209', '205') then '2A'
            when substring(offerer_postal_code, 0, 3) in ('202', '206') then '2B'
            else substring(offerer_postal_code, 0, 2)
        end as offerer_department_code
    from
        {{ source('raw', 'applicative_database_offerer') }} as offerer
    where
        "offerer_postal_code" is not NULL
),

related_venues as (
    select
        offerer.offerer_id,
        count(distinct venue_id) as total_venues_managed,
        coalesce(count(distinct case when not venue_is_virtual then venue_id else NULL end), 0) as physical_venues_managed,
        coalesce(count(distinct case when venue_is_permanent then venue_id else NULL end), 0) as permanent_venues_managed
    from
        {{ source('raw', 'applicative_database_offerer') }} as offerer
        left join {{ ref('venue') }} as venue on offerer.offerer_id = venue.venue_managing_offerer_id
    group by
        1
),

venues_with_offers as (
    select
        offerer.offerer_id,
        count(distinct offer.venue_id) as nb_venue_with_offers
    from
        {{ source('raw', 'applicative_database_offerer') }} as offerer
        left join {{ ref('venue') }} as venue on offerer.offerer_id = venue.venue_managing_offerer_id
        left join {{ ref('offer') }} as offer on venue.venue_id = offer.venue_id
    group by
        offerer_id
),

siren_reference_adage as (
    select
        siren,
        max(siren_synchro_adage) as siren_synchro_adage
    from {{ ref('adage') }}
    group by 1
),

dms_adage as (

    select
        * except (demandeur_entreprise_siren),
        case when demandeur_entreprise_siren is NULL or demandeur_entreprise_siren = "nan"
                then left(demandeur_siret, 9)
            else demandeur_entreprise_siren
        end as demandeur_entreprise_siren

    from {{ source('clean', 'dms_pro_cleaned') }}
    where procedure_id in ('57081', '57189', '61589', '65028', '80264')
),

first_dms_adage as (
    select *
    from dms_adage
    qualify row_number() over (partition by demandeur_entreprise_siren order by application_submitted_at asc) = 1
),

first_dms_adage_accepted as (
    select *
    from dms_adage
    where application_status = "accepte"
    qualify row_number() over (partition by demandeur_entreprise_siren order by processed_at asc) = 1
)


select
    offerer.offerer_id,
    concat("offerer-", offerer.offerer_id) as partner_id,
    offerer.offerer_name,
    offerer.offerer_creation_date,
    offerer.offerer_validation_date,
    related_stocks.first_stock_creation_date,
    individual_offers_per_offerer.first_individual_offer_creation_date as offerer_first_individual_offer_creation_date,
    individual_offers_per_offerer.last_individual_offer_creation_date as offerer_last_individual_offer_creation_date,
    collective_offers_per_offerer.first_collective_offer_creation_date as offerer_first_collective_offer_creation_date,
    collective_offers_per_offerer.last_collective_offer_creation_date as offerer_last_collective_offer_creation_date,
    case when first_individual_offer_creation_date is not NULL and first_collective_offer_creation_date is not NULL then least(first_collective_offer_creation_date, first_individual_offer_creation_date)
        when first_individual_offer_creation_date is not NULL then first_individual_offer_creation_date
        else first_collective_offer_creation_date
    end as offerer_first_offer_creation_date,
    case when last_individual_offer_creation_date is not NULL and last_collective_offer_creation_date is not NULL then greatest(last_collective_offer_creation_date, last_individual_offer_creation_date)
        when last_individual_offer_creation_date is not NULL then last_individual_offer_creation_date
        else last_collective_offer_creation_date
    end as offerer_last_offer_creation_date,
    bookable_offer_history.offerer_first_bookable_offer_date,
    bookable_offer_history.offerer_last_bookable_offer_date,
    bookable_offer_history.offerer_first_individual_bookable_offer_date,
    bookable_offer_history.offerer_last_individual_bookable_offer_date,
    bookable_offer_history.offerer_first_collective_bookable_offer_date,
    bookable_offer_history.offerer_last_collective_bookable_offer_date,
    individual_bookings_per_offerer.first_individual_booking_date as offerer_first_individual_booking_date,
    individual_bookings_per_offerer.last_individual_booking_date as offerer_last_individual_booking_date,
    collective_bookings_per_offerer.first_collective_booking_date as offerer_first_collective_booking_date,
    collective_bookings_per_offerer.last_collective_booking_date as offerer_last_collective_booking_date,
    case when first_individual_booking_date is not NULL and first_collective_booking_date is not NULL then least(first_collective_booking_date, first_individual_booking_date)
        when first_individual_booking_date is not NULL then first_individual_booking_date
        else first_collective_booking_date
    end as first_booking_date,
    case when last_individual_booking_date is not NULL and last_collective_booking_date is not NULL then greatest(last_collective_booking_date, last_individual_booking_date)
        when last_individual_booking_date is not NULL then last_individual_booking_date
        else last_collective_booking_date
    end as offerer_last_booking_date,
    coalesce(individual_offers_per_offerer.individual_offers_created, 0) as offerer_individual_offers_created,
    coalesce(collective_offers_per_offerer.collective_offers_created, 0) as offerer_collective_offers_created,
    coalesce(individual_offers_per_offerer.individual_offers_created, 0) + coalesce(collective_offers_per_offerer.collective_offers_created, 0) as offer_cnt,
    coalesce(bookable_individual_offer_cnt.offerer_bookable_individual_offer_cnt, 0) as offerer_bookable_individual_offer_cnt,
    coalesce(bookable_collective_offer_cnt.offerer_bookable_collective_offer_cnt, 0) as offerer_bookable_collective_offer_cnt,
    coalesce(bookable_individual_offer_cnt.offerer_bookable_individual_offer_cnt, 0) + coalesce(bookable_collective_offer_cnt.offerer_bookable_collective_offer_cnt, 0) as offerer_bookable_offer_cnt,
    coalesce(individual_bookings_per_offerer.non_cancelled_individual_bookings, 0) as offerer_non_cancelled_individual_bookings,
    coalesce(collective_bookings_per_offerer.non_cancelled_collective_bookings, 0) as offerer_non_cancelled_collective_bookings,
    coalesce(individual_bookings_per_offerer.non_cancelled_individual_bookings, 0) + coalesce(collective_bookings_per_offerer.non_cancelled_collective_bookings, 0) as no_cancelled_booking_cnt,
    coalesce(individual_bookings_per_offerer.used_individual_bookings, 0) + coalesce(collective_bookings_per_offerer.used_collective_bookings, 0) as offerer_used_bookings,
    coalesce(individual_bookings_per_offerer.used_individual_bookings, 0) as offerer_used_individual_bookings,
    coalesce(collective_bookings_per_offerer.used_collective_bookings, 0) as offerer_used_collective_bookings,
    coalesce(individual_bookings_per_offerer.individual_theoretic_revenue, 0) as offerer_individual_theoretic_revenue,
    coalesce(individual_bookings_per_offerer.individual_real_revenue, 0) as offerer_individual_real_revenue,
    coalesce(collective_bookings_per_offerer.collective_theoretic_revenue, 0) as offerer_collective_theoretic_revenue,
    coalesce(collective_bookings_per_offerer.collective_real_revenue, 0) as offerer_collective_real_revenue,
    coalesce(individual_bookings_per_offerer.individual_theoretic_revenue, 0) + coalesce(collective_bookings_per_offerer.collective_theoretic_revenue, 0) as offerer_theoretic_revenue,
    coalesce(individual_bookings_per_offerer.individual_real_revenue, 0) + coalesce(collective_bookings_per_offerer.collective_real_revenue, 0) as offerer_real_revenue,
    coalesce(individual_bookings_per_offerer.individual_current_year_real_revenue, 0) + coalesce(collective_bookings_per_offerer.collective_current_year_real_revenue, 0) as current_year_revenue,
    offerer_department_code.offerer_department_code,
    offerer.offerer_city,
    region_department.region_name as offerer_region_name,
    offerer.offerer_siren,
    siren_data.activiteprincipaleunitelegale as legal_unit_business_activity_code,
    label_unite_legale as legal_unit_business_activity_label,
    siren_data.categoriejuridiqueunitelegale as legal_unit_legal_category_code,
    label_categorie_juridique as legal_unit_legal_category_label,
    case when siren_data.activiteprincipaleunitelegale = '84.11Z' then TRUE else FALSE end as is_local_authority,
    total_venues_managed,
    physical_venues_managed,
    permanent_venues_managed,
    coalesce(venues_with_offers.nb_venue_with_offers, 0) as venue_with_offer,
    offerer_humanized_id.humanized_id as offerer_humanized_id,
    case when first_dms_adage.application_status is NULL then "dms_adage_non_depose" else first_dms_adage.application_status end as first_dms_adage_status,
    first_dms_adage_accepted.processed_at as dms_accepted_at,
    case when siren_reference_adage.siren is NULL then FALSE else TRUE end as is_reference_adage,
    case when siren_reference_adage.siren is NULL then FALSE else siren_synchro_adage end as is_synchro_adage
from
    {{ source('raw', 'applicative_database_offerer') }} as offerer
    left join individual_bookings_per_offerer on individual_bookings_per_offerer.offerer_id = offerer.offerer_id
    left join collective_bookings_per_offerer on collective_bookings_per_offerer.offerer_id = offerer.offerer_id
    left join individual_offers_per_offerer on individual_offers_per_offerer.offerer_id = offerer.offerer_id
    left join collective_offers_per_offerer on collective_offers_per_offerer.offerer_id = offerer.offerer_id
    left join related_stocks on related_stocks.offerer_id = offerer.offerer_id
    left join offerer_department_code on offerer_department_code.offerer_id = offerer.offerer_id
    left join {{ source('analytics', 'region_department') }} as region_department on offerer_department_code.offerer_department_code = region_department.num_dep
    left join related_venues on related_venues.offerer_id = offerer.offerer_id
    left join venues_with_offers on venues_with_offers.offerer_id = offerer.offerer_id
    left join offerer_humanized_id on offerer_humanized_id.offerer_id = offerer.offerer_id
    left join bookable_individual_offer_cnt on bookable_individual_offer_cnt.offerer_id = offerer.offerer_id
    left join bookable_collective_offer_cnt on bookable_collective_offer_cnt.offerer_id = offerer.offerer_id
    left join bookable_offer_history on bookable_offer_history.offerer_id = offerer.offerer_id
    left join {{ source('clean', 'siren_data') }} as siren_data on siren_data.siren = offerer.offerer_siren
    left join
        {{ source('analytics', 'siren_data_labels') }}
            as siren_data_labels
        on siren_data_labels.activiteprincipaleunitelegale = siren_data.activiteprincipaleunitelegale
            and cast(siren_data_labels.categoriejuridiqueunitelegale as STRING) = cast(siren_data.categoriejuridiqueunitelegale as STRING)
    left join first_dms_adage on first_dms_adage.demandeur_entreprise_siren = offerer.offerer_siren
    left join first_dms_adage_accepted on first_dms_adage_accepted.demandeur_entreprise_siren = offerer.offerer_siren
    left join siren_reference_adage on offerer.offerer_siren = siren_reference_adage.siren
where
    offerer.offerer_validation_status = 'VALIDATED'
    and offerer.offerer_is_active
qualify row_number() over (partition by offerer.offerer_siren order by update_date desc) = 1
