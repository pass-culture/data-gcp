{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(pre_hook="{{create_humanize_id_function()}}") }}

with
    offers_grouped_by_venue as (
        select
            venue_id,
            sum(total_individual_bookings) as total_individual_bookings,
            sum(
                total_non_cancelled_individual_bookings
            ) as total_non_cancelled_individual_bookings,
            sum(total_used_individual_bookings) as total_used_individual_bookings,
            sum(
                total_individual_theoretic_revenue
            ) as total_individual_theoretic_revenue,
            sum(total_individual_real_revenue) as total_individual_real_revenue,
            sum(
                total_individual_current_year_real_revenue
            ) as total_individual_current_year_real_revenue,
            min(first_individual_booking_date) as first_individual_booking_date,
            max(last_individual_booking_date) as last_individual_booking_date,
            min(first_stock_creation_date) as first_stock_creation_date,
            min(
                case when offer_validation = 'APPROVED' then offer_creation_date end
            ) as first_individual_offer_creation_date,
            max(
                case when offer_validation = 'APPROVED' then offer_creation_date end
            ) as last_individual_offer_creation_date,
            count(
                case when offer_validation = 'APPROVED' then offer_id end
            ) as total_created_individual_offers,
            count(
                distinct case when offer_is_bookable then offer_id end
            ) as total_bookable_individual_offers,
            count(distinct venue_id) as total_venues,
            count(
                distinct case when total_headlines > 0 then offer_id end
            ) as total_distinct_headline_offers,
            max(is_headlined) as has_headline_offer,
            min(first_headline_date) as first_headline_offer_date,
            max(last_headline_date) as last_headline_offer_date
        from {{ ref("int_applicative__offer") }}
        group by venue_id
    ),

    collective_offers_grouped_by_venue as (
        select
            venue_id,
            sum(total_collective_bookings) as total_collective_bookings,
            sum(
                total_non_cancelled_collective_bookings
            ) as total_non_cancelled_collective_bookings,
            sum(total_used_collective_bookings) as total_used_collective_bookings,
            sum(
                total_collective_theoretic_revenue
            ) as total_collective_theoretic_revenue,
            sum(total_collective_real_revenue) as total_collective_real_revenue,
            sum(
                total_collective_current_year_real_revenue
            ) as total_collective_current_year_real_revenue,
            min(first_collective_booking_date) as first_collective_booking_date,
            max(last_collective_booking_date) as last_collective_booking_date,
            sum(total_non_cancelled_tickets) as total_non_cancelled_tickets,
            sum(
                total_current_year_non_cancelled_tickets
            ) as total_current_year_non_cancelled_tickets,
            count(
                case
                    when collective_offer_validation = 'APPROVED'
                    then collective_offer_id
                end
            ) as total_created_collective_offers,
            min(
                case
                    when collective_offer_validation = 'APPROVED'
                    then collective_offer_creation_date
                end
            ) as first_collective_offer_creation_date,
            max(
                case
                    when collective_offer_validation = 'APPROVED'
                    then collective_offer_creation_date
                end
            ) as last_collective_offer_creation_date,
            count(
                distinct case
                    when collective_offer_is_bookable then collective_offer_id
                end
            ) as total_bookable_collective_offers
        from {{ ref("int_applicative__collective_offer") }}
        group by venue_id
    )

select
    v.venue_thumb_count,
    v.ban_id,
    v.venue_id,
    v.venue_name,
    v.venue_siret,
    v.venue_managing_offerer_id,
    v.venue_booking_email,
    v.venue_is_virtual,
    v.venue_comment,
    v.venue_public_name,
    v.venue_type_code as venue_type_label,
    v.venue_label_id,
    v.venue_creation_date,
    v.venue_is_permanent,
    v.venue_is_open_to_public,
    v.venue_is_soft_deleted,
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
    v.venue_adage_inscription_date,
    concat(
        'https://backoffice.passculture.team/pro/venue/', v.venue_id
    ) as venue_backoffice_link,
    {{ target_schema }}.humanize_id(v.venue_id) as venue_humanized_id,
    v_loc.venue_street,
    v_loc.venue_latitude,
    v_loc.venue_longitude,
    v_loc.venue_postal_code,
    v_loc.venue_department_code,
    v_loc.venue_department_name,
    v_loc.venue_iris_internal_id,
    v_loc.venue_region_name,
    v_loc.venue_city,
    v_loc.venue_epci,
    v_loc.venue_density_label,
    v_loc.venue_macro_density_label,
    v_loc.venue_density_level,
    v_loc.venue_academy_name,
    v_loc.venue_in_qpv,
    v_loc.venue_in_zrr,
    v_loc.venue_rural_city_type,
    v.offerer_address_id,
    vr.venue_target as venue_targeted_audience,
    vc.venue_contact_phone_number,
    vc.venue_contact_email,
    vc.venue_contact_website,
    vl.venue_label,
    va.venue_id is not null as venue_is_acessibility_synched,
    o.total_individual_bookings as total_individual_bookings,
    co.total_collective_bookings as total_collective_bookings,
    coalesce(o.total_individual_bookings, 0)
    + coalesce(co.total_collective_bookings, 0) as total_bookings,
    coalesce(
        o.total_non_cancelled_individual_bookings, 0
    ) as total_non_cancelled_individual_bookings,
    coalesce(
        co.total_non_cancelled_collective_bookings, 0
    ) as total_non_cancelled_collective_bookings,
    o.first_individual_booking_date,
    o.last_individual_booking_date,
    o.first_stock_creation_date,
    co.first_collective_booking_date,
    co.last_collective_booking_date,
    coalesce(o.total_non_cancelled_individual_bookings, 0) + coalesce(
        co.total_non_cancelled_collective_bookings, 0
    ) as total_non_cancelled_bookings,
    coalesce(o.total_used_individual_bookings, 0)
    + coalesce(co.total_used_collective_bookings, 0) as total_used_bookings,
    coalesce(o.total_used_individual_bookings, 0) as total_used_individual_bookings,
    coalesce(co.total_used_collective_bookings, 0) as total_used_collective_bookings,
    coalesce(
        o.total_individual_theoretic_revenue, 0
    ) as total_individual_theoretic_revenue,
    coalesce(o.total_individual_real_revenue, 0) as total_individual_real_revenue,
    coalesce(
        co.total_collective_theoretic_revenue, 0
    ) as total_collective_theoretic_revenue,
    coalesce(co.total_collective_real_revenue, 0) as total_collective_real_revenue,
    coalesce(
        o.total_individual_current_year_real_revenue, 0
    ) as total_individual_current_year_real_revenue,
    coalesce(
        co.total_collective_current_year_real_revenue, 0
    ) as total_collective_current_year_real_revenue,
    coalesce(o.total_individual_theoretic_revenue, 0)
    + coalesce(co.total_collective_theoretic_revenue, 0) as total_theoretic_revenue,
    coalesce(o.total_individual_real_revenue, 0)
    + coalesce(co.total_collective_real_revenue, 0) as total_real_revenue,
    o.first_individual_offer_creation_date,
    o.last_individual_offer_creation_date,
    coalesce(o.total_created_individual_offers, 0) as total_created_individual_offers,
    co.first_collective_offer_creation_date,
    co.last_collective_offer_creation_date,
    coalesce(co.total_created_collective_offers, 0) as total_created_collective_offers,
    coalesce(o.total_created_individual_offers, 0)
    + coalesce(co.total_created_collective_offers, 0) as total_created_offers,
    case
        when
            o.first_individual_booking_date is not null
            and co.first_collective_booking_date is not null
        then least(co.first_collective_booking_date, o.first_individual_booking_date)
        else coalesce(first_individual_booking_date, first_collective_booking_date)
    end as first_booking_date,
    case
        when
            o.last_individual_booking_date is not null
            and co.last_collective_booking_date is not null
        then greatest(co.last_collective_booking_date, o.last_individual_booking_date)
        else coalesce(o.last_individual_booking_date, co.last_collective_booking_date)
    end as last_booking_date,
    case
        when
            o.first_individual_offer_creation_date is not null
            and co.first_collective_offer_creation_date is not null
        then
            least(
                co.first_collective_offer_creation_date,
                o.first_individual_offer_creation_date
            )
        else
            coalesce(
                o.first_individual_offer_creation_date,
                co.first_collective_offer_creation_date
            )
    end as first_offer_creation_date,
    case
        when
            o.last_individual_offer_creation_date is not null
            and co.last_collective_offer_creation_date is not null
        then
            greatest(
                co.last_collective_offer_creation_date,
                o.last_individual_offer_creation_date
            )
        else
            coalesce(
                o.last_individual_offer_creation_date,
                co.last_collective_offer_creation_date
            )
    end as last_offer_creation_date,
    coalesce(o.total_bookable_individual_offers, 0) as total_bookable_individual_offers,
    coalesce(o.total_venues, 0) as total_venues,
    coalesce(
        co.total_bookable_collective_offers, 0
    ) as total_bookable_collective_offers,
    coalesce(o.total_bookable_individual_offers, 0)
    + coalesce(co.total_bookable_collective_offers, 0) as total_bookable_offers,
    coalesce(co.total_non_cancelled_tickets, 0) as total_non_cancelled_tickets,
    coalesce(
        co.total_current_year_non_cancelled_tickets, 0
    ) as total_current_year_non_cancelled_tickets,
    row_number() over (
        partition by venue_managing_offerer_id
        order by
            coalesce(o.total_individual_real_revenue, 0)
            + coalesce(co.total_collective_real_revenue, 0) desc
    ) as offerer_real_revenue_rank,
    row_number() over (
        partition by venue_managing_offerer_id
        order by
            coalesce(o.total_used_individual_bookings, 0)
            + coalesce(co.total_used_collective_bookings, 0) desc
    ) as offerer_bookings_rank,
    case
        when gp.banner_url is not null
        then "offerer"
        when gp.venue_id is not null
        then "google"
        else "default_category"
    end as venue_image_source,
    coalesce(o.total_distinct_headline_offers, 0) as total_distinct_headline_offers,
    coalesce(o.has_headline_offer, false) as has_headline_offer,
    o.first_headline_offer_date,
    o.last_headline_offer_date
from {{ source("raw", "applicative_database_venue") }} as v
left join {{ ref("int_geo__venue_location") }} as v_loc on v_loc.venue_id = v.venue_id
left join offers_grouped_by_venue as o on o.venue_id = v.venue_id
left join collective_offers_grouped_by_venue as co on co.venue_id = v.venue_id
left join
    {{ source("raw", "applicative_database_venue_registration") }} as vr
    on v.venue_id = vr.venue_id
left join
    {{ source("raw", "applicative_database_venue_contact") }} as vc
    on v.venue_id = vc.venue_id
left join
    {{ source("raw", "applicative_database_venue_label") }} as vl
    on vl.venue_label_id = v.venue_label_id
left join
    {{ source("raw", "applicative_database_accessibility_provider") }} as va
    on va.venue_id = v.venue_id
left join
    {{ source("raw", "applicative_database_google_places_info") }} as gp
    on v.venue_id = gp.venue_id
where not v.venue_is_soft_deleted
