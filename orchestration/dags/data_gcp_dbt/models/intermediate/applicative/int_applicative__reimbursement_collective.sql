select
    collective_booking.collective_booking_id,
    collective_booking.venue_id,
    collective_booking.offerer_id,
    collective_stock.collective_stock_price as booking_amount,
    collective_booking.collective_booking_status,
    (pricing_line.amount / 100) as offerer_contribution,
    - (pricing.amount / 100) - (pricing_line.amount / 100) as reimbursed_amount,
    cashflow_batch.cutoff as cashflow_ending_date,
    cashflow_batch.label as cashflow_label,
    invoice.invoice_id
from
    {{ source("raw", "applicative_database_collective_booking") }} as collective_booking
join
    {{ source("raw", "applicative_database_collective_stock") }} as collective_stock
    on collective_stock.collective_stock_id = collective_booking.collective_stock_id
    and collective_booking_status = 'REIMBURSED'
join
    {{ source("raw", "applicative_database_educational_institution") }} as institution
    on collective_booking.educational_institution_id
    = institution.educational_institution_id
join
    {{ source("raw", "applicative_database_venue") }} as venue
    on collective_booking.venue_id = venue.venue_id
join
    {{ source("raw", "applicative_database_pricing") }} pricing
    on collective_booking.collective_booking_id = pricing.collective_booking_id
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
