{{
    config(
        **custom_table_config(
            materialized="table",
            cluster_by=["offerer_siren", "venue_id"],
        )
    )
}}

with
    individual_data as (
        select
            offerer.offerer_siren,
            case
                when offer.offer_id is null then null else "INDIVIDUAL"
            end as individual_collective,
            venue.venue_id,
            venue.venue_name,
            venue.venue_public_name,
            subcategories.category_id,
            subcategories.id as subcategory,
            offer.offer_id,
            offer.offer_name,
            count(
                distinct case
                    when booking.booking_status in ("USED", "REIMBURSED")
                    then booking.booking_id
                end
            ) as count_bookings,  -- will be deleted
            count(
                distinct case
                    when booking.booking_status in ("USED", "REIMBURSED")
                    then booking.booking_id
                end
            ) as count_used_bookings,
            sum(
                case
                    when booking.booking_status in ("USED", "REIMBURSED")
                    then booking.booking_quantity
                end
            ) as count_used_tickets_booked,
            sum(
                case
                    when booking.booking_status in ("CONFIRMED")
                    then booking.booking_quantity
                end
            ) as count_pending_tickets_booked,
            count(
                distinct case
                    when booking.booking_status in ("CONFIRMED") then booking.booking_id
                end
            ) as count_pending_bookings,
            sum(
                case
                    when booking.booking_status in ("USED", "REIMBURSED")
                    then booking.booking_intermediary_amount
                end
            ) as real_amount_booked,
            sum(
                case
                    when booking.booking_status in ("CONFIRMED")
                    then booking.booking_intermediary_amount
                end
            ) as pending_amount_booked
        from {{ ref("mrt_global__venue") }} as venue
        inner join
            {{ ref("mrt_global__offerer") }} as offerer
            on venue.offerer_id = offerer.offerer_id
        left join
            {{ ref("mrt_global__offer") }} as offer on venue.venue_id = offer.venue_id
        left join
            {{ source("raw", "subcategories") }} as subcategories
            on offer.offer_subcategory_id = subcategories.id
        left join
            {{ ref("mrt_global__booking") }} as booking
            on offer.offer_id = booking.offer_id
            and booking.booking_status in ("USED", "REIMBURSED", "CONFIRMED")
        where offerer.offerer_siren is not null
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    ),

    collective_data as (
        select
            offerer.offerer_siren,
            case
                when offer.collective_offer_id is null then null else "COLLECTIVE"
            end as individual_collective,
            venue.venue_id,
            venue.venue_name,
            venue.venue_public_name,
            cast(null as string) as category_id,
            cast(null as string) as subcategory,
            offer.collective_offer_id,
            offer.collective_offer_name,
            count(
                distinct case
                    when
                        booking.collective_booking_status
                        in ("USED", "REIMBURSED", "CONFIRMED")
                    then booking.collective_booking_id
                end
            ) as count_bookings,  -- will be deleted
            count(
                distinct case
                    when
                        booking.collective_booking_status
                        in ("USED", "REIMBURSED", "CONFIRMED")
                    then booking.collective_booking_id
                end
            ) as count_used_bookings,
            count(
                distinct case
                    when booking.collective_booking_status in ("PENDING")
                    then booking.collective_booking_id
                end
            ) as count_pending_bookingst,
            count(
                distinct case
                    when
                        booking.collective_booking_status
                        in ("USED", "REIMBURSED", "CONFIRMED")
                    then booking.collective_booking_id
                end
            ) as count_used_tickets_booked,  -- 1 offer = 1 ticket, just to match format
            count(
                distinct case
                    when booking.collective_booking_status in ("PENDING")
                    then booking.collective_booking_id
                end
            ) as count_pending_tickets_booked,  -- same
            sum(
                case
                    when
                        booking.collective_booking_status
                        in ("USED", "REIMBURSED", "CONFIRMED")
                    then booking.booking_amount
                end
            ) as real_amount_booked,
            sum(
                case
                    when booking.collective_booking_status in ("PENDING")
                    then booking.booking_amount
                end
            ) as pending_amount_booked
        from {{ ref("mrt_global__venue") }} as venue
        inner join
            {{ ref("mrt_global__offerer") }} as offerer
            on venue.offerer_id = offerer.offerer_id
        left join
            {{ ref("mrt_global__collective_offer") }} as offer
            on venue.venue_id = offer.venue_id
        left join
            {{ ref("mrt_global__collective_booking") }} as booking
            on offer.collective_offer_id = booking.collective_offer_id
            and booking.collective_booking_status
            in ("USED", "REIMBURSED", "CONFIRMED", "PENDING")
        where offerer.offerer_siren is not null
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
