WITH booking_amount AS(
SELECT
    booking_id,
    offer_category_id,
    venue_department_code,
    venue_region_name,
    booking_used_date,
    booking_intermediary_amount,
    booking_quantity
FROM {{ ref('int_global__booking') }}
WHERE booking_is_used IS true
AND booking_used_date>="2021-01-01"
),

reimbursed_amount AS (
SELECT DISTINCT
    finance_event.booking_id AS indiv_bookingid,
    pricing_line.category AS amount_type,
    pricing.amount,
    cashflow.batchid,
    bfi.booking_id AS  indiv_incident_id
FROM {{ ref('int_finance__pricing') }} AS pricing
LEFT JOIN {{ ref('int_finance__event') }} AS finance_event ON pricing.event_id = finance_event.finance_event_id
LEFT JOIN {{ ref('int_finance__pricing_line') }} AS pricing_line ON pricing.id = pricing_line.pricingid
LEFT JOIN {{ ref('int_finance__cashflow_pricing') }} AS cash ON pricing.id = cash.pricingid
LEFT JOIN {{ ref('int_finance__cashflow') }} AS cashflow ON cash.cashflowid = cashflow.id
LEFT JOIN {{ ref('int_finance__booking_incident') }} AS bfi ON finance_event.booking_finance_incident_id = bfi.id
LEFT JOIN {{ ref('int_global__venue') }} AS venue ON pricing.venue_id = venue.venue_id
WHERE pricing.status="invoiced"
AND (pricing.bookingid IS NOT null OR bfi.booking_id IS NOT null) -- conserver uniquement le volet individuel
AND pricing_line.category ="offerer revenue"
),

contribution_amount AS (
SELECT DISTINCT
    finance_event.booking_id AS indiv_bookingid,
    pricing_line.category AS amount_type,
    pricing_line.amount,
    cashflow.batchid,
    bfi.booking_id AS  indiv_incident_id
FROM {{ ref('int_finance__pricing') }} AS pricing
LEFT JOIN {{ ref('int_finance__event') }} AS finance_event ON pricing.event_id = finance_event.finance_event_id
LEFT JOIN {{ ref('int_finance__pricing_line') }} AS pricing_line ON pricing.id = pricing_line.pricingid
LEFT JOIN {{ ref('int_finance__cashflow_pricing') }} AS cash ON pricing.id = cash.pricingid
LEFT JOIN {{ ref('int_finance__cashflow') }} AS cashflow ON cash.cashflowid = cashflow.id
LEFT JOIN {{ ref('int_finance__booking_incident') }} AS bfi ON finance_event.booking_finance_incident_id = bfi.id
LEFT JOIN {{ ref('int_global__venue') }} AS venue ON pricing.venue_id = venue.venue_id
WHERE pricing.status="invoiced"
AND (pricing.bookingid IS NOT null OR bfi.booking_id IS NOT null) -- conserver uniquement le volet individuel
AND pricing_line.category="offerer contribution"
)

SELECT
    booking_amount.booking_used_date,
    booking_amount.venue_department_code,
    booking_amount.venue_region_name,
    booking_amount.offer_category_id,
    count(DISTINCT booking_amount.booking_id) AS total_bookings,
    sum(booking_amount.booking_quantity) AS total_quantities,
    sum(booking_amount.booking_intermediary_amount) AS total_revenue_amount,
    -sum(reimbursed_amount.amount)/100 AS total_reimbused_amount,
    sum(contribution_amount.amount)/100 AS total_contribution_amount
FROM booking_amount
LEFT JOIN reimbursed_amount ON booking_amount.booking_id = coalesce(reimbursed_amount.indiv_bookingid, reimbursed_amount.indiv_incident_id)
LEFT JOIN contribution_amount ON booking_amount.booking_id = coalesce(contribution_amount.indiv_bookingid, contribution_amount.indiv_incident_id)
WHERE reimbursed_amount.batchid IS NOT null
AND contribution_amount.batchid IS NOT null
GROUP BY booking_amount.booking_used_date, booking_amount.venue_department_code, booking_amount.venue_region_name, booking_amount.offer_category_id
