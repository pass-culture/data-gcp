select distinct
    pricing_line.category as amount_type,
    cashflow.batchid,
    coalesce(finance_event.booking_id, bfi.booking_id) as booking_id,
    case
        when pricing_line.category = "offerer revenue"
        then - pricing.amount / 100
        when pricing_line.category = "offerer contribution"
        then pricing_line.amount / 100
        else 0
    end as amount
from {{ ref("int_finance__pricing") }} as pricing
left join
    {{ ref("int_finance__event") }} as finance_event
    on pricing.event_id = finance_event.finance_event_id
left join
    {{ ref("int_finance__pricing_line") }} as pricing_line
    on pricing.id = pricing_line.pricingid
left join
    {{ ref("int_finance__cashflow_pricing") }} as cash on pricing.id = cash.pricingid
left join
    {{ ref("int_finance__cashflow") }} as cashflow on cash.cashflowid = cashflow.id
left join
    {{ ref("int_finance__booking_incident") }} as bfi
    on finance_event.booking_finance_incident_id = bfi.id
where
    pricing.status = "invoiced"
    and (pricing.bookingid is not null or bfi.booking_id is not null)
    and pricing_line.category in ("offerer revenue", "offerer contribution")
    and cashflow.batchid is not null
