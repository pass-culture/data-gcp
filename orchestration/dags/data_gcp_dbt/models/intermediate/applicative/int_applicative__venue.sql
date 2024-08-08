{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

with offers_grouped_by_venue as (
    select
        venue_id,
        SUM(total_individual_bookings) as total_individual_bookings,
        SUM(total_non_cancelled_individual_bookings) as total_non_cancelled_individual_bookings,
        SUM(total_used_individual_bookings) as total_used_individual_bookings,
        SUM(total_individual_theoretic_revenue) as total_individual_theoretic_revenue,
        SUM(total_individual_real_revenue) as total_individual_real_revenue,
        SUM(total_individual_current_year_real_revenue) as total_individual_current_year_real_revenue,
        MIN(first_individual_booking_date) as first_individual_booking_date,
        MAX(last_individual_booking_date) as last_individual_booking_date,
        MIN(first_stock_creation_date) as first_stock_creation_date,
        MIN(case when offer_validation = "APPROVED" then offer_creation_date end) as first_individual_offer_creation_date,
        MAX(case when offer_validation = "APPROVED" then offer_creation_date end) as last_individual_offer_creation_date,
        COUNT(case when offer_validation = "APPROVED" then offer_id end) as total_created_individual_offers,
        COUNT(distinct case when offer_is_bookable then offer_id end) as total_bookable_individual_offers,
        COUNT(distinct venue_id) as total_venues
    from {{ ref("int_applicative__offer") }}
    group by venue_id
),

collective_offers_grouped_by_venue as (
    select
        venue_id,
        SUM(total_collective_bookings) as total_collective_bookings,
        SUM(total_non_cancelled_collective_bookings) as total_non_cancelled_collective_bookings,
        SUM(total_used_collective_bookings) as total_used_collective_bookings,
        SUM(total_collective_theoretic_revenue) as total_collective_theoretic_revenue,
        SUM(total_collective_real_revenue) as total_collective_real_revenue,
        SUM(total_collective_current_year_real_revenue) as total_collective_current_year_real_revenue,
        MIN(first_collective_booking_date) as first_collective_booking_date,
        MAX(last_collective_booking_date) as last_collective_booking_date,
        SUM(total_non_cancelled_tickets) as total_non_cancelled_tickets,
        SUM(total_current_year_non_cancelled_tickets) as total_current_year_non_cancelled_tickets,
        COUNT(case when collective_offer_validation = 'APPROVED' then collective_offer_id end) as total_created_collective_offers,
        MIN(case when collective_offer_validation = 'APPROVED' then collective_offer_creation_date end) as first_collective_offer_creation_date,
        MAX(case when collective_offer_validation = 'APPROVED' then collective_offer_creation_date end) as last_collective_offer_creation_date,
        COUNT(distinct case when collective_offer_is_bookable then collective_offer_id end) as total_bookable_collective_offers
    from {{ ref('int_applicative__collective_offer') }}
    group by venue_id
),

bookable_offer_history as (
    select
        venue_id,
        MIN(partition_date) as first_bookable_offer_date,
        MAX(partition_date) as last_bookable_offer_date,
        MIN(case when individual_bookable_offers > 0 then partition_date end) as first_individual_bookable_offer_date,
        MAX(case when individual_bookable_offers > 0 then partition_date end) as last_individual_bookable_offer_date,
        MIN(case when collective_bookable_offers > 0 then partition_date end) as first_collective_bookable_offer_date,
        MAX(case when collective_bookable_offers > 0 then partition_date end) as last_collective_bookable_offer_date
    from {{ ref('bookable_venue_history') }}
    group by venue_id
),

venues_with_geo_candidates as (
    select
        v.venue_id,
        v.venue_latitude,
        v.venue_longitude,
        gi.iris_internal_id,
        gi.region_name as venue_region_name,
        gi.city_label as venue_city,
        gi.epci_label as venue_epci,
        gi.academy_name as venue_academy_name,
        gi.density_label as venue_density_label,
        gi.density_macro_level as venue_macro_density_label,
        gi.iris_shape
    from {{ source("raw", "applicative_database_venue") }} as v
        left join {{ ref('int_seed__geo_iris') }} as gi
            on v.venue_longitude between gi.min_longitude and gi.max_longitude
                and v.venue_latitude between gi.min_latitude and gi.max_latitude
),

venue_geo_iris AS (
    SELECT *
    FROM venues_with_geo_candidates
    where ST_CONTAINS(
        iris_shape,
        ST_GEOGPOINT(venue_longitude, venue_latitude)
    ) or iris_shape is NULL
)

select
    v.venue_thumb_count,
    v.venue_street,
    v.venue_postal_code,
    v.ban_id,
    v.venue_id,
    v.venue_name,
    v.venue_siret,
    v.venue_latitude,
    v.venue_longitude,
    v.venue_managing_offerer_id,
    v.venue_booking_email,
    v.venue_is_virtual,
    v.venue_comment,
    v.venue_public_name,
    v.venue_type_code as venue_type_label,
    v.venue_label_id,
    v.venue_creation_date,
    v.venue_is_permanent,
    v.banner_url,
    v.venue_audiodisabilitycompliant,
    v.venue_mentaldisabilitycompliant,
    v.venue_motordisabilitycompliant,
    v.venue_visualdisabilitycompliant,
    v.venue_adage_id,
    v.venue_educational_status_id,
    v.collective_description,
    v.collective_students,
    v.collective_website,
    v.collective_network,
    v.collective_intervention_area,
    v.collective_access_information,
    v.collective_phone,
    v.collective_email,
    v.dms_token,
    v.venue_description,
    v.venue_withdrawal_details,
    COALESCE(
        case when v.venue_postal_code = "97150" then "978"
            when SUBSTRING(v.venue_postal_code, 0, 2) = "97" then SUBSTRING(v.venue_postal_code, 0, 3)
            when SUBSTRING(v.venue_postal_code, 0, 2) = "98" then SUBSTRING(v.venue_postal_code, 0, 3)
            when SUBSTRING(v.venue_postal_code, 0, 3) in ("200", "201", "209", "205") then "2A"
            when SUBSTRING(v.venue_postal_code, 0, 3) in ("202", "206") then "2B"
            else SUBSTRING(v.venue_postal_code, 0, 2)
        end,
        v.venue_department_code
    ) as venue_department_code,
    CONCAT(
        "https://backoffice.passculture.team/pro/venue/",
        v.venue_id
    ) as venue_backoffice_link,
    {{ target_schema }}.humanize_id(v.venue_id) as venue_humanized_id,
    venue_geo_iris.iris_internal_id as venue_iris_internal_id,
    venue_geo_iris.venue_region_name,
    venue_geo_iris.venue_city,
    venue_geo_iris.venue_epci,
    venue_geo_iris.venue_density_label,
    venue_geo_iris.venue_macro_density_label,
    venue_geo_iris.venue_academy_name,
    v.offerer_address_id,
    vr.venue_target as venue_targeted_audience,
    vc.venue_contact_phone_number,
    vc.venue_contact_email,
    vc.venue_contact_website,
    vl.venue_label,
    va.venue_id is not NULL as venue_is_acessibility_synched,
    o.total_individual_bookings as total_individual_bookings,
    co.total_collective_bookings as total_collective_bookings,
    COALESCE(o.total_individual_bookings, 0) + COALESCE(co.total_collective_bookings, 0) as total_bookings,
    COALESCE(o.total_non_cancelled_individual_bookings, 0) as total_non_cancelled_individual_bookings,
    COALESCE(co.total_non_cancelled_collective_bookings, 0) as total_non_cancelled_collective_bookings,
    o.first_individual_booking_date,
    o.last_individual_booking_date,
    o.first_stock_creation_date,
    co.first_collective_booking_date,
    co.last_collective_booking_date,
    COALESCE(o.total_non_cancelled_individual_bookings, 0) + COALESCE(co.total_non_cancelled_collective_bookings, 0) as total_non_cancelled_bookings,
    COALESCE(o.total_used_individual_bookings, 0) + COALESCE(co.total_used_collective_bookings, 0) as total_used_bookings,
    COALESCE(o.total_used_individual_bookings, 0) as total_used_individual_bookings,
    COALESCE(co.total_used_collective_bookings, 0) as total_used_collective_bookings,
    COALESCE(o.total_individual_theoretic_revenue, 0) as total_individual_theoretic_revenue,
    COALESCE(o.total_individual_real_revenue, 0) as total_individual_real_revenue,
    COALESCE(co.total_collective_theoretic_revenue, 0) as total_collective_theoretic_revenue,
    COALESCE(co.total_collective_real_revenue, 0) as total_collective_real_revenue,
    COALESCE(o.total_individual_current_year_real_revenue, 0) as total_individual_current_year_real_revenue,
    COALESCE(co.total_collective_current_year_real_revenue, 0) as total_collective_current_year_real_revenue,
    COALESCE(o.total_individual_theoretic_revenue, 0) + COALESCE(co.total_collective_theoretic_revenue, 0) as total_theoretic_revenue,
    COALESCE(o.total_individual_real_revenue, 0) + COALESCE(co.total_collective_real_revenue, 0) as total_real_revenue,
    o.first_individual_offer_creation_date,
    o.last_individual_offer_creation_date,
    COALESCE(o.total_created_individual_offers, 0) as total_created_individual_offers,
    co.first_collective_offer_creation_date,
    co.last_collective_offer_creation_date,
    COALESCE(co.total_created_collective_offers, 0) as total_created_collective_offers,
    COALESCE(o.total_created_individual_offers, 0) + COALESCE(co.total_created_collective_offers, 0) as total_created_offers,
    boh.first_bookable_offer_date,
    boh.last_bookable_offer_date,
    boh.first_individual_bookable_offer_date,
    boh.last_individual_bookable_offer_date,
    boh.first_collective_bookable_offer_date,
    boh.last_collective_bookable_offer_date,
    case when o.first_individual_booking_date is not NULL and co.first_collective_booking_date is not NULL then LEAST(co.first_collective_booking_date, o.first_individual_booking_date)
        else COALESCE(first_individual_booking_date, first_collective_booking_date)
    end as first_booking_date,
    case when o.last_individual_booking_date is not NULL and co.last_collective_booking_date is not NULL then GREATEST(co.last_collective_booking_date, o.last_individual_booking_date)
        else COALESCE(o.last_individual_booking_date, co.last_collective_booking_date)
    end as last_booking_date,
    case when o.first_individual_offer_creation_date is not NULL and co.first_collective_offer_creation_date is not NULL then LEAST(co.first_collective_offer_creation_date, o.first_individual_offer_creation_date)
        else COALESCE(o.first_individual_offer_creation_date, co.first_collective_offer_creation_date)
    end as first_offer_creation_date,
    case when o.last_individual_offer_creation_date is not NULL and co.last_collective_offer_creation_date is not NULL then GREATEST(co.last_collective_offer_creation_date, o.last_individual_offer_creation_date)
        else COALESCE(o.last_individual_offer_creation_date, co.last_collective_offer_creation_date)
    end as last_offer_creation_date,
    COALESCE(o.total_bookable_individual_offers, 0) as total_bookable_individual_offers,
    COALESCE(o.total_venues, 0) as total_venues,
    COALESCE(co.total_bookable_collective_offers, 0) as total_bookable_collective_offers,
    COALESCE(o.total_bookable_individual_offers, 0) + COALESCE(co.total_bookable_collective_offers, 0) as total_bookable_offers,
    COALESCE(co.total_non_cancelled_tickets, 0) as total_non_cancelled_tickets,
    COALESCE(co.total_current_year_non_cancelled_tickets, 0) as total_current_year_non_cancelled_tickets,
    case when DATE_DIFF(CURRENT_DATE, boh.last_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, boh.last_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_active_current_year,
    case when DATE_DIFF(CURRENT_DATE, boh.last_individual_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_individual_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, boh.last_individual_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_individual_active_current_year,
    case when DATE_DIFF(CURRENT_DATE, boh.last_collective_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_collective_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, boh.last_collective_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_collective_active_current_year,
    ROW_NUMBER() over (
        partition by venue_managing_offerer_id
        order by
            COALESCE(o.total_individual_real_revenue, 0) + COALESCE(co.total_collective_real_revenue, 0) desc
    ) as offerer_real_revenue_rank,
    ROW_NUMBER() over (
        partition by venue_managing_offerer_id
        order by
            COALESCE(o.total_used_individual_bookings, 0) + COALESCE(co.total_used_collective_bookings, 0) desc
    ) as offerer_bookings_rank,
    case when gp.banner_url is not NULL then "offerer" when gp.venue_id is not NULL then "google" else "default_category" end as venue_image_source
from {{ source("raw", "applicative_database_venue") }} as v
    left join venue_geo_iris on venue_geo_iris.venue_id = v.venue_id
    left join offers_grouped_by_venue as o on o.venue_id = v.venue_id
    left join collective_offers_grouped_by_venue as co on co.venue_id = v.venue_id
    left join bookable_offer_history as boh on boh.venue_id = v.venue_id
    left join {{ source("raw", "applicative_database_venue_registration") }} as vr on v.venue_id = vr.venue_id
    left join {{ source("raw", "applicative_database_venue_contact") }} as vc on v.venue_id = vc.venue_id
    left join {{ source("raw", "applicative_database_venue_label") }} as vl on vl.venue_label_id = v.venue_label_id
    left join {{ source("raw", "applicative_database_accessibility_provider") }} as va on va.venue_id = v.venue_id
    left join {{ source("raw", "applicative_database_google_places_info") }} AS gp ON v.venue_id = gp.venue_id
