SELECT
    booking.booking_id
    , booking.venue_id
    , booking.offerer_id
    , booking.booking_amount as booking_amount
    , booking.booking_status
    , (pricing_line.amount/100) AS offerer_contribution
    , -(pricing.amount/100)-(pricing_line.amount/100) as reimbursed_amount
    , cashflow_batch.cutoff AS cashflow_ending_date
    , cashflow_batch.label AS cashflow_label
    , invoice.invoice_id
FROM {{ ref('booking') }} as booking
JOIN {{ source('raw', 'applicative_database_venue') }} as venue
    ON  booking.venue_id = venue.venue_id
JOIN {{ source('raw', 'applicative_database_pricing') }} pricing
    ON  booking.booking_id = pricing.bookingId
    AND pricing.status='invoiced'
JOIN {{ source('raw', 'applicative_database_pricing_line') }} pricing_line
    ON  pricing.id = pricing_line.pricingId
    AND category = 'offerer contribution'
JOIN {{ source('raw', 'applicative_database_cashflow_pricing') }} cashflow_pricing
    ON  pricing.id = cashflow_pricing.pricingId
JOIN {{ source('raw', 'applicative_database_cashflow') }} cashflow
    ON  cashflow_pricing.cashflowId = cashflow.id
JOIN {{ source('raw', 'applicative_database_cashflow_batch') }} cashflow_batch
    ON  cashflow.batchId = cashflow_batch.id
JOIN {{ source('raw', 'applicative_database_invoice_cashflow') }} invoice_cashflow
    ON  cashflow.id = invoice_cashflow.cashflow_id
JOIN {{ source('raw', 'applicative_database_invoice') }} invoice
    ON  invoice_cashflow.invoice_id = invoice.invoice_id
WHERE booking_status = 'REIMBURSED'