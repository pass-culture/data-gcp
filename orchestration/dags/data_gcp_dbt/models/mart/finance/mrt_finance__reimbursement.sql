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

    reimbursed_amount as (
        select distinct
            finance_event.booking_id as indiv_bookingid,
            pricing_line.category as amount_type,
            pricing.amount,
            cashflow.batchid,
            bfi.booking_id as indiv_incident_id
        from {{ ref("int_finance__pricing") }} as pricing
        left join
            {{ ref("int_finance__event") }} as finance_event
            on pricing.event_id = finance_event.finance_event_id
        left join
            {{ ref("int_finance__pricing_line") }} as pricing_line
            on pricing.id = pricing_line.pricingid
        left join
            {{ ref("int_finance__cashflow_pricing") }} as cash
            on pricing.id = cash.pricingid
        left join
            {{ ref("int_finance__cashflow") }} as cashflow
            on cash.cashflowid = cashflow.id
        left join
            {{ ref("int_finance__booking_incident") }} as bfi
            on finance_event.booking_finance_incident_id = bfi.id
        left join
            {{ ref("int_global__venue") }} as venue on pricing.venue_id = venue.venue_id
        where
            pricing.status = "invoiced"
            and (pricing.bookingid is not null or bfi.booking_id is not null)  -- conserver uniquement le volet individuel
            and pricing_line.category = "offerer revenue"
    ),

    contribution_amount as (
        select distinct
            finance_event.booking_id as indiv_bookingid,
            pricing_line.category as amount_type,
            pricing_line.amount,
            cashflow.batchid,
            bfi.booking_id as indiv_incident_id
        from {{ ref("int_finance__pricing") }} as pricing
        left join
            {{ ref("int_finance__event") }} as finance_event
            on pricing.event_id = finance_event.finance_event_id
        left join
            {{ ref("int_finance__pricing_line") }} as pricing_line
            on pricing.id = pricing_line.pricingid
        left join
            {{ ref("int_finance__cashflow_pricing") }} as cash
            on pricing.id = cash.pricingid
        left join
            {{ ref("int_finance__cashflow") }} as cashflow
            on cash.cashflowid = cashflow.id
        left join
            {{ ref("int_finance__booking_incident") }} as bfi
            on finance_event.booking_finance_incident_id = bfi.id
        left join
            {{ ref("int_global__venue") }} as venue on pricing.venue_id = venue.venue_id
        where
            pricing.status = "invoiced"
            and (pricing.bookingid is not null or bfi.booking_id is not null)  -- conserver uniquement le volet individuel
            and pricing_line.category = "offerer contribution"
    )

select
    booking_amount.booking_used_date,
    booking_amount.venue_department_code,
    booking_amount.venue_region_name,
    booking_amount.offer_category_id,
    count(distinct booking_amount.booking_id) as total_bookings,
    sum(booking_amount.booking_quantity) as total_quantities,
    sum(booking_amount.booking_intermediary_amount) as total_revenue_amount,
    - sum(reimbursed_amount.amount) / 100 as total_reimbused_amount,
    sum(contribution_amount.amount) / 100 as total_contribution_amount
from booking_amount
left join
    reimbursed_amount
    on booking_amount.booking_id
    = coalesce(reimbursed_amount.indiv_bookingid, reimbursed_amount.indiv_incident_id)
left join
    contribution_amount
    on booking_amount.booking_id = coalesce(
        contribution_amount.indiv_bookingid, contribution_amount.indiv_incident_id
    )
where reimbursed_amount.batchid is not null and contribution_amount.batchid is not null
group by
    booking_amount.booking_used_date,
    booking_amount.venue_department_code,
    booking_amount.venue_region_name,
    booking_amount.offer_category_id
