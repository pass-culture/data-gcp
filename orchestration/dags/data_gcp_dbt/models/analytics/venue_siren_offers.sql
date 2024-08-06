with individual_data as (
    select
        offerer_siren,
        case when offer.offer_id is NULL then NULL else "INDIVIDUAL" end as individual_collective,
        venue.venue_id,
        venue.venue_name,
        venue.venue_public_name,
        subcategories.category_id,
        subcategories.id as subcategory,
        offer.offer_id,
        offer.offer_name,
        COUNT(distinct case when booking.booking_status in ('USED', 'REIMBURSED') then booking.booking_id else NULL end) as count_bookings, -- will be deleted
        COUNT(distinct case when booking.booking_status in ('USED', 'REIMBURSED') then booking.booking_id else NULL end) as count_used_bookings,
        SUM(case when booking.booking_status in ('USED', 'REIMBURSED') then booking.booking_quantity else NULL end) as count_used_tickets_booked,
        SUM(case when booking.booking_status in ('CONFIRMED') then booking.booking_quantity else NULL end) as count_pending_tickets_booked,
        COUNT(distinct case when booking.booking_status in ('CONFIRMED') then booking.booking_id else NULL end) as count_pending_bookings,
        SUM(case when booking.booking_status in ('USED', 'REIMBURSED') then booking.booking_intermediary_amount else NULL end) as real_amount_booked,
        SUM(case when booking.booking_status in ('CONFIRMED') then booking.booking_intermediary_amount else NULL end) as pending_amount_booked
    from {{ ref('mrt_global__venue') }} venue
        join {{ ref('enriched_offerer_data') }} offerer on venue.venue_managing_offerer_id = offerer.offerer_id
        left join {{ ref('mrt_global__offer') }} offer on venue.venue_id = offer.venue_id
        left join {{ source('clean','subcategories') }} subcategories on offer.offer_subcategory_id = subcategories.id
        left join {{ ref('mrt_global__booking') }} booking on offer.offer_id = booking.offer_id and booking.booking_status in ('USED', 'REIMBURSED', 'CONFIRMED')
    where offerer_siren is not NULL
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

collective_data as (
    select
        offerer_siren,
        case when offer.collective_offer_id is NULL then NULL else "COLLECTIVE" end as individual_collective,
        venue.venue_id,
        venue.venue_name,
        venue.venue_public_name,
        offer.collective_offer_category_id,
        offer.collective_offer_subcategory_id,
        offer.collective_offer_id,
        offer.collective_offer_name,
        COUNT(distinct case when booking.collective_booking_status in ('USED', 'REIMBURSED', 'CONFIRMED') then booking.collective_booking_id else NULL end) as count_bookings, -- will be deleted
        COUNT(distinct case when booking.collective_booking_status in ('USED', 'REIMBURSED', 'CONFIRMED') then booking.collective_booking_id else NULL end) as count_used_bookings,
        COUNT(distinct case when booking.collective_booking_status in ('PENDING') then booking.collective_booking_id else NULL end) as count_pending_bookingst,
        COUNT(distinct case when booking.collective_booking_status in ('USED', 'REIMBURSED', 'CONFIRMED') then booking.collective_booking_id else NULL end) as count_used_tickets_booked, -- 1 offer = 1 ticket, just to match format
        COUNT(distinct case when booking.collective_booking_status in ('PENDING') then booking.collective_booking_id else NULL end) as count_pending_tickets_booked, -- same
        SUM(case when booking.collective_booking_status in ('USED', 'REIMBURSED', 'CONFIRMED') then booking.booking_amount else NULL end) as real_amount_booked,
        SUM(case when booking.collective_booking_status in ('PENDING') then booking.booking_amount else NULL end) as pending_amount_booked
    from {{ ref('mrt_global__venue') }} venue
        join {{ ref('enriched_offerer_data') }} offerer on venue.venue_managing_offerer_id = offerer.offerer_id
        left join {{ ref('mrt_global__collective_offer') }} offer on venue.venue_id = offer.venue_id
        left join {{ ref('mrt_global__collective_booking') }} booking on offer.collective_offer_id = booking.collective_offer_id and booking.collective_booking_status in ('USED', 'REIMBURSED', 'CONFIRMED', 'PENDING')
    where offerer_siren is not NULL
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
),

all_data as (
    select *
    from individual_data
    union all
    select *
    from collective_data
)

select *
from all_data
