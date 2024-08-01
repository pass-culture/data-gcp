{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

with venue_grouped_by_offerer as (
    select
        venue_managing_offerer_id,
        SUM(total_non_cancelled_individual_bookings) as total_non_cancelled_individual_bookings,
        SUM(total_used_individual_bookings) as total_used_individual_bookings,
        SUM(total_individual_theoretic_revenue) as total_individual_theoretic_revenue,
        SUM(total_individual_real_revenue) as total_individual_real_revenue,
        SUM(total_individual_current_year_real_revenue) as total_individual_current_year_real_revenue,
        MIN(first_individual_booking_date) as first_individual_booking_date,
        MAX(last_individual_booking_date) as last_individual_booking_date,
        MIN(first_individual_offer_creation_date) as first_individual_offer_creation_date,
        MAX(last_individual_offer_creation_date) as last_individual_offer_creation_date,
        SUM(total_created_individual_offers) as total_created_individual_offers,
        SUM(total_bookable_individual_offers) as total_bookable_individual_offers,
        MIN(first_stock_creation_date) as first_stock_creation_date,
        SUM(total_non_cancelled_collective_bookings) as total_non_cancelled_collective_bookings,
        SUM(total_used_collective_bookings) as total_used_collective_bookings,
        SUM(total_collective_theoretic_revenue) as total_collective_theoretic_revenue,
        SUM(total_collective_real_revenue) as total_collective_real_revenue,
        SUM(total_collective_current_year_real_revenue) as total_collective_current_year_real_revenue,
        MIN(first_collective_booking_date) as first_collective_booking_date,
        MAX(last_collective_booking_date) as last_collective_booking_date,
        MIN(first_collective_offer_creation_date) as first_collective_offer_creation_date,
        MAX(last_collective_offer_creation_date) as last_collective_offer_creation_date,
        SUM(total_created_collective_offers) as total_created_collective_offers,
        SUM(total_bookable_collective_offers) as total_bookable_collective_offers,
        SUM(total_venues) as total_venues,
        SUM(total_non_cancelled_bookings) as total_non_cancelled_bookings,
        SUM(total_used_bookings) as total_used_bookings,
        SUM(total_theoretic_revenue) as total_theoretic_revenue,
        SUM(total_real_revenue) as total_real_revenue,
        SUM(total_created_offers) as total_created_offers,
        SUM(total_bookable_offers) as total_bookable_offers,
        MIN(first_bookable_offer_date) as first_bookable_offer_date,
        MAX(last_bookable_offer_date) as last_bookable_offer_date,
        MIN(first_individual_bookable_offer_date) as first_individual_bookable_offer_date,
        MAX(last_individual_bookable_offer_date) as last_individual_bookable_offer_date,
        MIN(first_collective_bookable_offer_date) as first_collective_bookable_offer_date,
        MAX(last_collective_bookable_offer_date) as last_collective_bookable_offer_date,
        COUNT(distinct venue_id) as total_managed_venues,
        COUNT(distinct case when not venue_is_virtual then venue_id end) as total_physical_managed_venues,
        COUNT(distinct case when venue_is_permanent then venue_id end) as total_permanent_managed_venues,
        STRING_AGG(distinct CONCAT(" ", case when venue_type_label != "Offre numérique" then venue_type_label end)) as all_physical_venues_types,
        COUNT(case when venue_type_label = "Lieu administratif" then venue_id else NULL end) as total_administrative_venues,
        MAX(case when offerer_real_revenue_rank = 1 then venue_type_label end) as top_real_revenue_venue_type,
        MAX(case when offerer_bookings_rank = 1 then venue_type_label end) as top_bookings_venue_type
    from {{ ref("int_applicative__venue") }}
    group by venue_managing_offerer_id
)

select
    o.offerer_is_active,
    o.offerer_address,
    o.offerer_postal_code,
    o.offerer_city,
    o.offerer_id,
    CONCAT("offerer-", o.offerer_id) as partner_id,
    o.offerer_creation_date,
    o.offerer_name,
    o.offerer_siren,
    o.offerer_validation_status,
    o.offerer_validation_date,
    {{ target_schema }}.humanize_id(o.offerer_id) as offerer_humanized_id,
    case
        when o.offerer_postal_code = "97150" then "978"
        when SUBSTRING(o.offerer_postal_code, 0, 2) = "97" then SUBSTRING(o.offerer_postal_code, 0, 3)
        when SUBSTRING(o.offerer_postal_code, 0, 2) = "98" then SUBSTRING(o.offerer_postal_code, 0, 3)
        when SUBSTRING(o.offerer_postal_code, 0, 3) in ("200", "201", "209", "205") then "2A"
        when SUBSTRING(o.offerer_postal_code, 0, 3) in ("202", "206") then "2B"
        else SUBSTRING(offerer_postal_code, 0, 2)
    end as offerer_department_code,
    COALESCE(vgo.total_non_cancelled_individual_bookings, 0) as total_non_cancelled_individual_bookings,
    COALESCE(vgo.total_used_individual_bookings, 0) as total_used_individual_bookings,
    COALESCE(vgo.total_individual_theoretic_revenue, 0) as total_individual_theoretic_revenue,
    COALESCE(vgo.total_individual_real_revenue, 0) as total_individual_real_revenue,
    vgo.first_individual_booking_date,
    vgo.last_individual_booking_date,
    vgo.first_individual_offer_creation_date,
    vgo.last_individual_offer_creation_date,
    COALESCE(vgo.total_bookable_individual_offers, 0) as total_bookable_individual_offers,
    COALESCE(vgo.total_bookable_collective_offers, 0) as total_bookable_collective_offers,
    COALESCE(vgo.total_created_individual_offers, 0) as total_created_individual_offers,
    COALESCE(vgo.total_created_collective_offers, 0) as total_created_collective_offers,
    vgo.first_stock_creation_date,
    COALESCE(vgo.total_non_cancelled_collective_bookings, 0) as total_non_cancelled_collective_bookings,
    COALESCE(vgo.total_used_collective_bookings, 0) as total_used_collective_bookings,
    COALESCE(vgo.total_collective_theoretic_revenue, 0) as total_collective_theoretic_revenue,
    COALESCE(vgo.total_collective_real_revenue, 0) as total_collective_real_revenue,
    COALESCE(vgo.total_individual_current_year_real_revenue, 0) + COALESCE(vgo.total_collective_current_year_real_revenue, 0) as total_current_year_real_revenue,
    vgo.first_collective_booking_date,
    vgo.last_collective_booking_date,
    vgo.first_collective_offer_creation_date,
    vgo.last_collective_offer_creation_date,
    COALESCE(vgo.total_non_cancelled_bookings) as total_non_cancelled_bookings,
    COALESCE(vgo.total_used_bookings, 0) as total_used_bookings,
    COALESCE(vgo.total_theoretic_revenue, 0) as total_theoretic_revenue,
    COALESCE(vgo.total_real_revenue, 0) as total_real_revenue,
    COALESCE(vgo.total_created_offers, 0) as total_created_offers,
    COALESCE(vgo.total_bookable_offers, 0) as total_bookable_offers,
    vgo.first_bookable_offer_date,
    vgo.last_bookable_offer_date,
    vgo.first_individual_bookable_offer_date,
    vgo.last_individual_bookable_offer_date,
    vgo.first_collective_bookable_offer_date,
    vgo.last_collective_bookable_offer_date,
    vgo.total_venues,
    vgo.top_real_revenue_venue_type,
    vgo.top_bookings_venue_type,
    COALESCE(vgo.total_managed_venues, 0) as total_managed_venues,
    COALESCE(vgo.total_physical_managed_venues, 0) as total_physical_managed_venues,
    COALESCE(vgo.total_permanent_managed_venues, 0) as total_permanent_managed_venues,
    vgo.total_administrative_venues,
    vgo.all_physical_venues_types,
    case when vgo.first_individual_offer_creation_date is not NULL and vgo.first_collective_offer_creation_date is not NULL then LEAST(vgo.first_collective_offer_creation_date, vgo.first_individual_offer_creation_date)
        else COALESCE(vgo.first_individual_offer_creation_date, vgo.first_collective_offer_creation_date)
    end as first_offer_creation_date,
    case when vgo.last_individual_offer_creation_date is not NULL and vgo.last_collective_offer_creation_date is not NULL then LEAST(vgo.last_collective_offer_creation_date, vgo.last_individual_offer_creation_date)
        else COALESCE(vgo.last_individual_offer_creation_date, vgo.last_collective_offer_creation_date)
    end as last_offer_creation_date,
    case when vgo.first_individual_booking_date is not NULL and vgo.first_collective_booking_date is not NULL then LEAST(vgo.first_collective_booking_date, vgo.first_individual_booking_date)
        else COALESCE(vgo.first_individual_booking_date, vgo.first_collective_booking_date)
    end as first_booking_date,
    case when vgo.last_individual_booking_date is not NULL and vgo.last_collective_booking_date is not NULL then LEAST(vgo.last_collective_booking_date, vgo.last_individual_booking_date)
        else COALESCE(vgo.last_individual_booking_date, vgo.last_collective_booking_date)
    end as last_booking_date,
    case when DATE_DIFF(CURRENT_DATE, vgo.last_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, vgo.last_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_active_current_year,
    case when DATE_DIFF(CURRENT_DATE, vgo.last_individual_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_individual_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, vgo.last_individual_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_individual_active_current_year,
    case when DATE_DIFF(CURRENT_DATE, vgo.last_collective_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_collective_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, vgo.last_collective_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_collective_active_current_year
from {{ source("raw", "applicative_database_offerer") }} as o
    left join venue_grouped_by_offerer as vgo on o.offerer_id = vgo.venue_managing_offerer_id
