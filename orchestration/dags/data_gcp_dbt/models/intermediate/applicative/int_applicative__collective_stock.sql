with
    collective_bookings_grouped_by_collective_stock as (
        select
            cb.collective_stock_id,
            count(
                case
                    when cb.collective_booking_status != 'CANCELLED'
                    then cb.collective_booking_id
                end
            ) as total_non_cancelled_collective_bookings,
            count(cb.collective_booking_id) as total_collective_bookings,
            count(
                case when is_used_collective_booking then collective_booking_id end
            ) as total_used_collective_bookings,
            min(cb.collective_booking_creation_date) as first_collective_booking_date,
            max(cb.collective_booking_creation_date) as last_collective_booking_date,
            sum(
                case
                    when cb.collective_booking_status != 'CANCELLED'
                    then cs.collective_stock_price
                end
            ) as total_collective_theoretic_revenue,
            sum(
                case
                    when cb.collective_booking_status in ('USED', 'REIMBURSED')
                    then cs.collective_stock_price
                end
            ) as total_collective_real_revenue,
            sum(
                case
                    when
                        collective_booking_status in ('USED', 'REIMBURSED')
                        and extract(year from collective_booking_creation_date)
                        = extract(year from current_date)
                    then cs.collective_stock_price
                end
            ) as total_collective_current_year_real_revenue,
            count(
                case
                    when
                        is_current_educational_year
                        and cb.collective_booking_status != 'CANCELLED'
                    then cb.collective_booking_id
                end
            ) as total_current_year_non_cancelled_collective_bookings
        from {{ ref("int_applicative__collective_booking") }} as cb
        left join
            {{ source("raw", "applicative_database_collective_stock") }} as cs
            on cb.collective_stock_id = cs.collective_stock_id
        group by collective_stock_id
    )

select
    cs.collective_stock_id,
    cs.collective_stock_creation_date,
    cs.collective_stock_modification_date,
    cs.collective_stock_beginning_date_time,
    cs.collective_stock_end_date_time,
    cs.collective_offer_id,
    cs.collective_stock_price,
    cs.collective_stock_booking_limit_date_time,
    cs.collective_stock_number_of_tickets,
    cs.collective_stock_price_detail,
    bcs.total_non_cancelled_collective_bookings,
    bcs.total_collective_bookings,
    bcs.total_used_collective_bookings,
    bcs.first_collective_booking_date,
    bcs.last_collective_booking_date,
    bcs.total_collective_theoretic_revenue,
    bcs.total_collective_real_revenue,
    bcs.total_collective_current_year_real_revenue,
    bcs.total_current_year_non_cancelled_collective_bookings,
    case
        when
            total_non_cancelled_collective_bookings is null
            and (
                date(cs.collective_stock_booking_limit_date_time)
                > (date("{{ ds() }}") - 1)
                or cs.collective_stock_booking_limit_date_time is null
            )
            and (
                date(cs.collective_stock_beginning_date_time) > (date("{{ ds() }}") - 1)
                or cs.collective_stock_beginning_date_time is null
            )
        then true
        else false
    end as collective_stock_is_bookable
from {{ source("raw", "applicative_database_collective_stock") }} as cs
left join
    collective_bookings_grouped_by_collective_stock as bcs
    on bcs.collective_stock_id = cs.collective_stock_id
