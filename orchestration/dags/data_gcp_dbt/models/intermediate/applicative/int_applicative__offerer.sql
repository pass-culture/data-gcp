{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(pre_hook="{{create_humanize_id_function()}}") }}

with
    venue_grouped_by_offerer as (
        select
            venue_managing_offerer_id,
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
            min(
                first_individual_offer_creation_date
            ) as first_individual_offer_creation_date,
            max(
                last_individual_offer_creation_date
            ) as last_individual_offer_creation_date,
            sum(total_created_individual_offers) as total_created_individual_offers,
            sum(total_bookable_individual_offers) as total_bookable_individual_offers,
            min(first_stock_creation_date) as first_stock_creation_date,
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
            min(
                first_collective_offer_creation_date
            ) as first_collective_offer_creation_date,
            max(
                last_collective_offer_creation_date
            ) as last_collective_offer_creation_date,
            sum(total_created_collective_offers) as total_created_collective_offers,
            sum(total_bookable_collective_offers) as total_bookable_collective_offers,
            sum(total_venues) as total_venues,
            sum(total_non_cancelled_bookings) as total_non_cancelled_bookings,
            sum(total_used_bookings) as total_used_bookings,
            sum(total_theoretic_revenue) as total_theoretic_revenue,
            sum(total_real_revenue) as total_real_revenue,
            sum(total_created_offers) as total_created_offers,
            sum(total_bookable_offers) as total_bookable_offers,
            count(distinct venue_id) as total_managed_venues,
            count(
                distinct case when not venue_is_virtual then venue_id end
            ) as total_physical_managed_venues,
            count(
                distinct case when venue_is_open_to_public then venue_id end
            ) as total_permanent_managed_venues,
            string_agg(
                distinct concat(
                    " ",
                    case
                        when venue_type_label != "Offre numérique" then venue_type_label
                    end
                )
            ) as all_physical_venues_types,
            count(
                case when venue_type_label = "Lieu administratif" then venue_id end
            ) as total_administrative_venues,
            max(
                case when offerer_real_revenue_rank = 1 then venue_type_label end
            ) as top_real_revenue_venue_type,
            max(
                case when offerer_bookings_rank = 1 then venue_type_label end
            ) as top_bookings_venue_type
        from {{ ref("int_applicative__venue") }}
        group by venue_managing_offerer_id
    )

select
    o.offerer_is_active,
    o.offerer_id,
    concat("offerer-", o.offerer_id) as partner_id,
    o.offerer_creation_date,
    o.offerer_name,
    o.offerer_siren,
    o.offerer_validation_status,
    o.offerer_validation_date,
    {{ target_schema }}.humanize_id(o.offerer_id) as offerer_humanized_id,
    coalesce(
        vgo.total_non_cancelled_individual_bookings, 0
    ) as total_non_cancelled_individual_bookings,
    coalesce(vgo.total_used_individual_bookings, 0) as total_used_individual_bookings,
    coalesce(
        vgo.total_individual_theoretic_revenue, 0
    ) as total_individual_theoretic_revenue,
    coalesce(vgo.total_individual_real_revenue, 0) as total_individual_real_revenue,
    vgo.first_individual_booking_date,
    vgo.last_individual_booking_date,
    vgo.first_individual_offer_creation_date,
    vgo.last_individual_offer_creation_date,
    coalesce(
        vgo.total_bookable_individual_offers, 0
    ) as total_bookable_individual_offers,
    coalesce(
        vgo.total_bookable_collective_offers, 0
    ) as total_bookable_collective_offers,
    coalesce(vgo.total_created_individual_offers, 0) as total_created_individual_offers,
    coalesce(vgo.total_created_collective_offers, 0) as total_created_collective_offers,
    vgo.first_stock_creation_date,
    coalesce(
        vgo.total_non_cancelled_collective_bookings, 0
    ) as total_non_cancelled_collective_bookings,
    coalesce(vgo.total_used_collective_bookings, 0) as total_used_collective_bookings,
    coalesce(
        vgo.total_collective_theoretic_revenue, 0
    ) as total_collective_theoretic_revenue,
    coalesce(vgo.total_collective_real_revenue, 0) as total_collective_real_revenue,
    coalesce(vgo.total_individual_current_year_real_revenue, 0) + coalesce(
        vgo.total_collective_current_year_real_revenue, 0
    ) as total_current_year_real_revenue,
    vgo.first_collective_booking_date,
    vgo.last_collective_booking_date,
    vgo.first_collective_offer_creation_date,
    vgo.last_collective_offer_creation_date,
    coalesce(vgo.total_non_cancelled_bookings) as total_non_cancelled_bookings,
    coalesce(vgo.total_used_bookings, 0) as total_used_bookings,
    coalesce(vgo.total_theoretic_revenue, 0) as total_theoretic_revenue,
    coalesce(vgo.total_real_revenue, 0) as total_real_revenue,
    coalesce(vgo.total_created_offers, 0) as total_created_offers,
    coalesce(vgo.total_bookable_offers, 0) as total_bookable_offers,
    vgo.total_venues,
    vgo.top_real_revenue_venue_type,
    vgo.top_bookings_venue_type,
    coalesce(vgo.total_managed_venues, 0) as total_managed_venues,
    coalesce(vgo.total_physical_managed_venues, 0) as total_physical_managed_venues,
    coalesce(vgo.total_permanent_managed_venues, 0) as total_permanent_managed_venues,
    vgo.total_administrative_venues,
    vgo.all_physical_venues_types,
    case
        when
            vgo.first_individual_offer_creation_date is not null
            and vgo.first_collective_offer_creation_date is not null
        then
            least(
                vgo.first_collective_offer_creation_date,
                vgo.first_individual_offer_creation_date
            )
        else
            coalesce(
                vgo.first_individual_offer_creation_date,
                vgo.first_collective_offer_creation_date
            )
    end as first_offer_creation_date,
    case
        when
            vgo.last_individual_offer_creation_date is not null
            and vgo.last_collective_offer_creation_date is not null
        then
            least(
                vgo.last_collective_offer_creation_date,
                vgo.last_individual_offer_creation_date
            )
        else
            coalesce(
                vgo.last_individual_offer_creation_date,
                vgo.last_collective_offer_creation_date
            )
    end as last_offer_creation_date,
    case
        when
            vgo.first_individual_booking_date is not null
            and vgo.first_collective_booking_date is not null
        then least(vgo.first_collective_booking_date, vgo.first_individual_booking_date)
        else
            coalesce(
                vgo.first_individual_booking_date, vgo.first_collective_booking_date
            )
    end as first_booking_date,
    case
        when
            vgo.last_individual_booking_date is not null
            and vgo.last_collective_booking_date is not null
        then least(vgo.last_collective_booking_date, vgo.last_individual_booking_date)
        else
            coalesce(vgo.last_individual_booking_date, vgo.last_collective_booking_date)
    end as last_booking_date,
from {{ ref("int_raw__offerer") }} as o
left join
    venue_grouped_by_offerer as vgo on o.offerer_id = vgo.venue_managing_offerer_id
