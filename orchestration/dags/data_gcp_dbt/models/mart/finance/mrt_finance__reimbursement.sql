with
    booking_amount as (
        select
            booking_id,
            offer_category_id,
            venue_department_code,
            venue_region_name,
            booking_used_date,
            booking_intermediary_amount,
            booking_quantity
        from {{ ref("int_global__booking") }}
        where booking_is_used is true and booking_used_date >= "2021-01-01"
    ),

    financial_amounts as (
        select distinct
            pricing_line.category as amount_type,
            cashflow.batchid,
            coalesce(finance_event.booking_id, bfi.booking_id) as booking_id,
            case
                when pricing_line.category = "offerer revenue" then -pricing.amount / 100
                when pricing_line.category = "offerer contribution" then pricing_line.amount / 100
                else 0
            end as amount
        from {{ ref("int_finance__pricing") }} as pricing
        left join {{ ref("int_finance__event") }} as finance_event
            on pricing.event_id = finance_event.finance_event_id
        left join {{ ref("int_finance__pricing_line") }} as pricing_line
            on pricing.id = pricing_line.pricingid
        left join {{ ref("int_finance__cashflow_pricing") }} as cash
            on pricing.id = cash.pricingid
        left join {{ ref("int_finance__cashflow") }} as cashflow
            on cash.cashflowid = cashflow.id
        left join {{ ref("int_finance__booking_incident") }} as bfi
            on finance_event.booking_finance_incident_id = bfi.id
        where
            pricing.status = "invoiced"
            and (pricing.bookingid is not null or bfi.booking_id is not null)
            and pricing_line.category in ("offerer revenue", "offerer contribution")
            and cashflow.batchid is not null
    ),

    -- Agrégation des montants par booking_id et type
    aggregated_amounts as (
        select
            booking_id,
            sum(case when amount_type = "offerer revenue" then amount else 0 end) as total_reimbursed_amount,
            sum(case when amount_type = "offerer contribution" then amount else 0 end) as total_contribution_amount
        from financial_amounts
        group by booking_id
    )

select
    booking_amount.booking_used_date,
    booking_amount.venue_department_code,
    booking_amount.venue_region_name,
    booking_amount.offer_category_id,
    count(distinct booking_amount.booking_id) as total_bookings,
    sum(booking_amount.booking_quantity) as total_quantities,
    sum(booking_amount.booking_intermediary_amount) as total_revenue_amount,
    sum(aggregated_amounts.total_reimbursed_amount) as total_reimbursed_amount,
    sum(aggregated_amounts.total_contribution_amount) as total_contribution_amount
from booking_amount
inner join aggregated_amounts
    on booking_amount.booking_id = aggregated_amounts.booking_id
group by
    booking_amount.booking_used_date,
    booking_amount.venue_department_code,
    booking_amount.venue_region_name,
    booking_amount.offer_category_id
