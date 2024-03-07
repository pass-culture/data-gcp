SELECT 
    collective_booking.collective_booking_id 
    , collective_booking.venue_id 
    , collective_booking.offerer_id
    , collective_stock.collective_stock_price as booking_amount 
    , collective_booking.collective_booking_status
    , (pricing_line.amount/100) AS offerer_contribution
    , -(pricing.amount/100)-(pricing_line.amount/100) as reimbursed_amount
    , cashflow_batch.cutoff AS cashflow_ending_date 
    , cashflow_batch.label AS cashflow_label 
    , invoice.invoice_id 
FROM {{ source('raw', 'applicative_database_collective_booking') }} as collective_booking 
JOIN {{ source('raw', 'applicative_database_collective_stock') }} as collective_stock
    ON collective_stock.collective_stock_id = collective_booking.collective_stock_id
    AND collective_booking_status = 'REIMBURSED' 
JOIN {{ source('raw', 'applicative_database_educational_institution') }} as institution
    ON  collective_booking.educational_institution_id = institution.educational_institution_id 
JOIN {{ ref('venue') }} as venue
    ON  collective_booking.venue_id = venue.venue_id 
JOIN {{ source('raw', 'applicative_database_pricing') }} pricing 
    ON  collective_booking.collective_booking_id = pricing.collective_booking_id
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