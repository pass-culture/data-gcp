with partner_activation as (
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
        case when partner.total_created_individual_offers > 0 then TRUE else FALSE end as has_activated_individual_part,
        case when partner.total_created_collective_offers > 0 then TRUE else FALSE end as has_activated_collective_part,
        case when partner_status = "venue" then venue.first_individual_offer_creation_date
            else offerer.first_individual_offer_creation_date
        end as individual_activation_date,
        case when partner_status = "venue" then venue.first_collective_offer_creation_date
            else offerer.first_collective_offer_creation_date
        end as collective_activation_date,
        partner.first_offer_creation_date as first_activation_date,
        partner.total_non_cancelled_individual_bookings as individual_bookings_after_first_activation,
        partner.total_non_cancelled_collective_bookings as collective_bookings_after_first_activation
    from {{ ref('mrt_global__cultural_partner') }} partner
        left join {{ ref('mrt_global__venue') }} venue on partner.venue_id = venue.venue_id
        left join {{ ref('mrt_global__offerer') }} offerer on offerer.offerer_id = partner.offerer_id
),

partner_activation_stated as (
    select
        partner_activation.*,
        case
            when not has_activated_individual_part then "collective"
            when not has_activated_collective_part then "individual"
            when individual_activation_date < collective_activation_date then "individual"
            else "collective"
        end as first_activated_part,
        case
            when not has_activated_individual_part then NULL
            when not has_activated_collective_part then NULL
            when individual_activation_date < collective_activation_date then collective_activation_date
            else individual_activation_date
        end as second_activation_date,
        case
            when (not has_activated_individual_part or not has_activated_collective_part) then NULL
            when individual_activation_date < collective_activation_date then DATE_DIFF(collective_activation_date, individual_activation_date, day)
            else DATE_DIFF(individual_activation_date, collective_activation_date, day)
        end as days_between_second_activation,
        case
            when not has_activated_individual_part then "collective only"
            when not has_activated_collective_part then "individual only"
            else "all part"
        end as activation_state

    from partner_activation
    where (has_activated_individual_part or has_activated_collective_part)
),

indiv_bookings_after_activation as (
    select
        partner_activation_stated.partner_id,
        COUNT(distinct case when booking_created_at > second_activation_date then booking_id end) as individual_bookings_after_second_activation

    from partner_activation_stated
        left join {{ ref('mrt_global__booking') }} as mrt_global__booking on partner_activation_stated.partner_id = mrt_global__booking.partner_id and not booking_is_cancelled
    group by 1
),

collec_bookings_after_activation as (
    select
        partner_activation_stated.partner_id,
        COUNT(distinct case when collective_booking_creation_date > second_activation_date then collective_booking_id end) as collective_bookings_after_second_activation

    from partner_activation_stated
        left join {{ ref('mrt_global__collective_booking') }} as mrt_global__collective_booking on partner_activation_stated.partner_id = mrt_global__collective_booking.partner_id and NOT collective_booking_is_cancelled
    group by 1
),

indiv_offers_after_activation as (
    select
        partner_activation_stated.partner_id,
        COUNT(distinct mrt_global__offer.offer_id) as individual_offers_created_after_first_activation,
        COUNT(distinct case when offer_created_at > second_activation_date then mrt_global__offer.offer_id end) as individual_offers_created_after_second_activation
    from partner_activation_stated
        left join {{ ref('mrt_global__offer') }} as mrt_global__offer on partner_activation_stated.partner_id = mrt_global__offer.partner_id
    group by 1
),

collec_offers_after_activation as (
    select
        partner_activation_stated.partner_id,
        COUNT(distinct mrt_global__collective_offer.collective_offer_id) as collective_offers_created_after_first_activation,
        COUNT(distinct case when collective_offer_creation_date > second_activation_date then mrt_global__collective_offer.collective_offer_id end) as collective_offers_created_after_second_activation
    from partner_activation_stated
        left join {{ ref('mrt_global__collective_offer') }} AS mrt_global__collective_offer on partner_activation_stated.partner_id = mrt_global__collective_offer.partner_id
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
    individual_bookings_after_first_activation - individual_bookings_after_second_activation as individual_bookings_between_activations,
    collective_bookings_after_first_activation - collective_bookings_after_second_activation as collective_bookings_between_activations,
    individual_offers_created_after_first_activation - individual_offers_created_after_second_activation as individual_offers_created_between_activations,
    collective_offers_created_after_first_activation - collective_offers_created_after_second_activation as collective_offers_created_between_activations
from partner_activation_stated
    left join indiv_bookings_after_activation on partner_activation_stated.partner_id = indiv_bookings_after_activation.partner_id
    left join collec_bookings_after_activation on partner_activation_stated.partner_id = collec_bookings_after_activation.partner_id
    left join indiv_offers_after_activation on partner_activation_stated.partner_id = indiv_offers_after_activation.partner_id
    left join collec_offers_after_activation on partner_activation_stated.partner_id = collec_offers_after_activation.partner_id
