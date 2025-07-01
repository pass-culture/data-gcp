select
    booking.booking_id,
    booking.venue_id,
    booking.offerer_id,
    booking.booking_amount as booking_amount,
    booking.booking_status,
    (pricing_line.amount / 100) as offerer_contribution,
    - (pricing.amount / 100) - (pricing_line.amount / 100) as reimbursed_amount,
    cashflow_batch.cutoff as cashflow_ending_date,
    cashflow_batch.label as cashflow_label,
    invoice.invoice_id
from {{ ref("int_applicative__booking") }} as booking
join
    {{ source("raw", "applicative_database_venue") }} as venue
    on booking.venue_id = venue.venue_id
join
    {{ source("raw", "applicative_database_pricing") }} pricing
    on booking.booking_id = pricing.bookingid
    and pricing.status = 'invoiced'
join
    {{ source("raw", "applicative_database_pricing_line") }} pricing_line
    on pricing.id = pricing_line.pricingid
    and category = 'offerer contribution'
join
    {{ source("raw", "applicative_database_cashflow_pricing") }} cashflow_pricing
    on pricing.id = cashflow_pricing.pricingid
join
    {{ source("raw", "applicative_database_cashflow") }} cashflow
    on cashflow_pricing.cashflowid = cashflow.id
join
    {{ source("raw", "applicative_database_cashflow_batch") }} cashflow_batch
    on cashflow.batchid = cashflow_batch.id
join
    {{ source("raw", "applicative_database_invoice_cashflow") }} invoice_cashflow
    on cashflow.id = invoice_cashflow.cashflow_id
join
    {{ source("raw", "applicative_database_invoice") }} invoice
    on invoice_cashflow.invoice_id = invoice.invoice_id
where booking_status = 'REIMBURSED'
