WITH clean_payment_status AS (
    SELECT
        paymentId,
        MAX(date) AS last_status_date
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_payment_status payment_statuts
    GROUP BY
        1
),
clean_pricing_line1 AS (
    SELECT
        booking.booking_id,
CASE
            WHEN category = 'offerer revenue' THEN pricing_line.amount
        END AS offerer_revenue,
CASE
            WHEN category = 'offerer contribution' THEN pricing_line.amount
        END AS offerer_contribution
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_booking_data booking
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_pricing pricing ON booking.booking_id = pricing.bookingId
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_pricing_line pricing_line ON pricing.id = pricing_line.pricingId
),
clean_pricing_line2 AS (
    SELECT
        booking_id,
        SUM(- offerer_revenue / 100) AS offerer_revenue,
        SUM(offerer_contribution / 100) AS offerer_contribution
    FROM
        clean_pricing_line1
    GROUP BY
        1
),
individuel AS (
    SELECT
        DISTINCT applicative_database_deposit.type AS booking_type,
        '' AS collective_booking_id,
        booking.booking_id,
        booking.booking_status,
        booking.booking_creation_date,
        booking_cancellation_date,
        booking_used_date,
        booking.venue_id,
        booking.offerer_id,
        '' AS educational_institution_id,
        pricing.id AS pricing_id,
        pricing.status AS pricing_status,
        pricing.creationdate AS pricing_creation_date,
        - pricing.amount / 100 AS pricing_amount,
        pricing.standardRule AS pricing_rule,
        clean_pricing_line2.offerer_revenue,
        clean_pricing_line2.offerer_contribution,
        cashflow.id AS cashflow_id,
        cashflow.creationdate AS cashflow_creation_date,
        cashflow.status AS cashflow_status,
        - cashflow.amount / 100 AS cashflow_amount,
        cashflow_batch.creationdate AS cashflow_batch_creation_date,
        cashflow_batch.label AS cashflow_batch_label,
        invoice.invoice_id,
        invoice.invoice_reference,
        invoice.invoice_creation_date,
        invoice.amount AS invoice_amount
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_booking booking
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock ON booking.stock_id = applicative_database_stock.stock_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_deposit ON booking.deposit_id = applicative_database_deposit.id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_pricing pricing ON booking.booking_id = pricing.bookingId
        LEFT JOIN clean_pricing_line2 ON booking.booking_id = clean_pricing_line2.booking_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_cashflow_pricing cashflow_pricing ON pricing.id = cashflow_pricing.pricingId
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_cashflow cashflow ON cashflow_pricing.cashflowId = cashflow.id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_cashflow_batch cashflow_batch ON cashflow.batchId = cashflow_batch.id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_invoice_cashflow invoice_cashflow ON cashflow.id = invoice_cashflow.cashflow_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_invoice invoice ON invoice_cashflow.invoice_id = invoice.invoice_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_invoice_line invoice_line ON invoice.invoice_id = invoice_line.invoice_id
    WHERE
        NOT invoice.invoice_reference LIKE '%.2'
),
coll_clean_pricing_line1 AS (
    SELECT
        collective_booking.collective_booking_id,
CASE
            WHEN category = 'offerer revenue' THEN pricing_line.amount
        END AS offerer_revenue,
CASE
            WHEN category = 'offerer contribution' THEN pricing_line.amount
        END AS offerer_contribution
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_collective_booking_data collective_booking
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_pricing pricing ON collective_booking.collective_booking_id = pricing.collective_booking_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_pricing_line pricing_line ON pricing.id = pricing_line.pricingId
),
coll_clean_pricing_line2 AS (
    SELECT
        collective_booking_id,
        SUM(- offerer_revenue / 100) AS offerer_revenue,
        SUM(offerer_contribution / 100) AS offerer_contribution
    FROM
        coll_clean_pricing_line1
    GROUP BY
        1
),
collective AS (
    SELECT
        'collective' AS booking_type,
        collective_booking.collective_booking_id,
        '' AS booking_id,
        collective_booking.collective_booking_status,
        collective_booking.collective_booking_creation_date,
        collective_booking.collective_booking_cancellation_date,
        collective_booking.collective_booking_used_date,
        collective_booking.venue_id,
        collective_booking.offerer_id,
        collective_booking.educational_institution_id,
        pricing.id AS pricing_id,
        pricing.status AS pricing_status,
        pricing.creationdate AS pricing_creation_date,
        - pricing.amount / 100 AS pricing_amount,
        pricing.standardRule AS pricing_rule,
        offerer_revenue,
        offerer_contribution,
        cashflow.id AS cashflow_id,
        cashflow.creationdate AS cashflow_creation_date,
        cashflow.status AS cashflow_status,
        - cashflow.amount / 100 AS cashflow_amount,
        cashflow_batch.creationdate AS cashflow_batch_creation_date,
        cashflow_batch.label AS cashflow_batch_label,
        invoice.invoice_id,
        invoice.invoice_reference,
        invoice.invoice_creation_date,
        invoice.amount AS invoice_amount
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_collective_booking collective_booking
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_pricing pricing ON collective_booking.collective_booking_id = pricing.collective_booking_id
        AND booking_id IS NULL
        LEFT JOIN coll_clean_pricing_line2 ON collective_booking.collective_booking_id = coll_clean_pricing_line2.collective_booking_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_cashflow_pricing cashflow_pricing ON pricing.id = cashflow_pricing.pricingId
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_cashflow cashflow ON cashflow_pricing.cashflowId = cashflow.id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_cashflow_batch cashflow_batch ON cashflow.batchId = cashflow_batch.id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_invoice_cashflow invoice_cashflow ON cashflow.id = invoice_cashflow.cashflow_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_invoice invoice ON invoice_cashflow.invoice_id = invoice.invoice_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_invoice_line invoice_line ON invoice.invoice_id = invoice_line.invoice_id
    WHERE
        NOT invoice.invoice_reference LIKE '%.2'
)
SELECT
    *
FROM
    individuel
UNION
ALL
SELECT
    *
FROM
    collective