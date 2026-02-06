with
    partner_activation as (
        select
            partner.partner_id,
            partner.venue_id,
            partner.offerer_id,
            partner.partner_creation_date,
            partner.partner_name,
            partner.partner_academy_name,
            partner.partner_department_code,
            partner.partner_region_name,
            partner.partner_status,
            partner.partner_type,
            partner.cultural_sector,
            offerer.legal_unit_business_activity_label,
            offerer.legal_unit_legal_category_label,
            partner.first_offer_creation_date as first_activation_date,
            partner.total_non_cancelled_individual_bookings
            as individual_bookings_after_first_activation,
            partner.total_non_cancelled_collective_bookings
            as collective_bookings_after_first_activation,
            coalesce(
                partner.total_created_individual_offers > 0, false
            ) as has_activated_individual_part,
            coalesce(
                partner.total_created_collective_offers > 0, false
            ) as has_activated_collective_part,
            case
                when partner_status = "venue"
                then venue.first_individual_offer_creation_date
                else offerer.first_individual_offer_creation_date
            end as individual_activation_date,
            case
                when partner_status = "venue"
                then venue.first_collective_offer_creation_date
                else offerer.first_collective_offer_creation_date
            end as collective_activation_date
        from {{ ref("mrt_global__cultural_partner") }} as partner
        left join
            {{ ref("mrt_global__venue") }} as venue on partner.venue_id = venue.venue_id
        left join
            {{ ref("mrt_global__offerer") }} as offerer
            on partner.offerer_id = offerer.offerer_id
    ),

    partner_activation_stated as (
        select
            partner_activation.*,
            case
                when not has_activated_individual_part
                then "collective"
                when not has_activated_collective_part
                then "individual"
                when individual_activation_date < collective_activation_date
                then "individual"
                else "collective"
            end as first_activated_part,
            case
                when not has_activated_individual_part
                then null
                when not has_activated_collective_part
                then null
                when individual_activation_date < collective_activation_date
                then collective_activation_date
                else individual_activation_date
            end as second_activation_date,
            case
                when
                    (
                        not has_activated_individual_part
                        or not has_activated_collective_part
                    )
                then null
                when individual_activation_date < collective_activation_date
                then
                    date_diff(
                        collective_activation_date, individual_activation_date, day
                    )
                else
                    date_diff(
                        individual_activation_date, collective_activation_date, day
                    )
            end as days_between_second_activation,
            case
                when not has_activated_individual_part
                then "collective only"
                when not has_activated_collective_part
                then "individual only"
                else "all part"
            end as activation_state

        from partner_activation
        where (has_activated_individual_part or has_activated_collective_part)
    ),

    indiv_bookings_after_activation as (
        select
            partner_activation_stated.partner_id,
            count(
                distinct case
                    when booking_created_at > second_activation_date then booking_id
                end
            ) as individual_bookings_after_second_activation

        from partner_activation_stated
        left join
            {{ ref("mrt_global__booking") }} as mrt_global__booking
            on partner_activation_stated.partner_id = mrt_global__booking.partner_id
            and not booking_is_cancelled
        group by 1
    ),

    collec_bookings_after_activation as (
        select
            partner_activation_stated.partner_id,
            count(
                distinct case
                    when collective_booking_creation_date > second_activation_date
                    then collective_booking_id
                end
            ) as collective_bookings_after_second_activation

        from partner_activation_stated
        left join
            {{ ref("mrt_global__collective_booking") }}
            as mrt_global__collective_booking
            on partner_activation_stated.partner_id
            = mrt_global__collective_booking.partner_id
            and not collective_booking_is_cancelled
        group by 1
    ),

    indiv_offers_after_activation as (
        select
            partner_activation_stated.partner_id,
            count(
                distinct mrt_global__offer.offer_id
            ) as individual_offers_created_after_first_activation,
            count(
                distinct case
                    when offer_created_at > second_activation_date
                    then mrt_global__offer.offer_id
                end
            ) as individual_offers_created_after_second_activation
        from partner_activation_stated
        left join
            {{ ref("mrt_global__offer") }} as mrt_global__offer
            on partner_activation_stated.partner_id = mrt_global__offer.partner_id
        group by 1
    ),

    collec_offers_after_activation as (
        select
            partner_activation_stated.partner_id,
            count(
                distinct mrt_global__collective_offer.collective_offer_id
            ) as collective_offers_created_after_first_activation,
            count(
                distinct case
                    when collective_offer_creation_date > second_activation_date
                    then mrt_global__collective_offer.collective_offer_id
                end
            ) as collective_offers_created_after_second_activation
        from partner_activation_stated
        left join
            {{ ref("mrt_global__collective_offer") }} as mrt_global__collective_offer
            on partner_activation_stated.partner_id
            = mrt_global__collective_offer.partner_id
        group by 1
    )

select
    partner_activation_stated.*,
    indiv_bookings_after_activation.individual_bookings_after_second_activation,
    collec_bookings_after_activation.collective_bookings_after_second_activation,
    indiv_offers_after_activation.individual_offers_created_after_first_activation,
    indiv_offers_after_activation.individual_offers_created_after_second_activation,
    collec_offers_after_activation.collective_offers_created_after_first_activation,
    collec_offers_after_activation.collective_offers_created_after_second_activation,
    individual_bookings_after_first_activation
    - individual_bookings_after_second_activation
    as individual_bookings_between_activations,
    collective_bookings_after_first_activation
    - collective_bookings_after_second_activation
    as collective_bookings_between_activations,
    individual_offers_created_after_first_activation
    - individual_offers_created_after_second_activation
    as individual_offers_created_between_activations,
    collective_offers_created_after_first_activation
    - collective_offers_created_after_second_activation
    as collective_offers_created_between_activations
from partner_activation_stated
left join
    indiv_bookings_after_activation
    on partner_activation_stated.partner_id = indiv_bookings_after_activation.partner_id
left join
    collec_bookings_after_activation
    on partner_activation_stated.partner_id
    = collec_bookings_after_activation.partner_id
left join
    indiv_offers_after_activation
    on partner_activation_stated.partner_id = indiv_offers_after_activation.partner_id
left join
    collec_offers_after_activation
    on partner_activation_stated.partner_id = collec_offers_after_activation.partner_id
