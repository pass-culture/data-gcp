with
    clean_payment_status as (
        select paymentid, max(date) as last_status_date
        from {{ ref("int_finance__payment_status") }}
        group by 1
    ),

    clean_pricing_line1 as (
        select
            booking.booking_id,
            case
                when category = 'offerer revenue' then pricing_line.amount
            end as offerer_revenue,
            case
                when category = 'offerer contribution' then pricing_line.amount
            end as offerer_contribution
        from {{ ref("int_applicative__booking") }} as booking
        left join
            {{ ref("int_finance__pricing") }} as pricing
            on booking.booking_id = pricing.bookingid
        left join
            {{ ref("int_finance__pricing_line") }} as pricing_line
            on pricing.id = pricing_line.pricingid
    ),

    clean_pricing_line2 as (
        select
            booking_id,
            sum(- offerer_revenue / 100) as offerer_revenue,
            sum(offerer_contribution / 100) as offerer_contribution
        from clean_pricing_line1
        group by 1
    ),

    individuel as (
        select distinct
            deposit.deposit_type as booking_type,
            '' as collective_booking_id,
            booking.booking_id,
            booking.booking_status,
            booking.booking_creation_date,
            booking_cancellation_date,
            booking_used_date,
            booking.venue_id,
            booking.offerer_id,
            '' as educational_institution_id,
            pricing.id as pricing_id,
            pricing.status as pricing_status,
            pricing.creationdate as pricing_creation_date,
            - pricing.amount / 100 as pricing_amount,
            pricing.standardrule as pricing_rule,
            clean_pricing_line2.offerer_revenue,
            clean_pricing_line2.offerer_contribution,
            cashflow.id as cashflow_id,
            cashflow.creationdate as cashflow_creation_date,
            cashflow.status as cashflow_status,
            - cashflow.amount / 100 as cashflow_amount,
            cashflow_batch.creationdate as cashflow_batch_creation_date,
            cashflow_batch.label as cashflow_batch_label,
            invoice.invoice_id,
            invoice.invoice_reference,
            invoice.invoice_creation_date,
            invoice.amount as invoice_amount
        from {{ ref("int_applicative__booking") }} as booking
        left join
            {{ ref("int_global__stock") }} as stock on booking.stock_id = stock.stock_id
        left join
            {{ ref("int_global__deposit") }} as deposit
            on booking.deposit_id = deposit.deposit_id
        left join
            {{ ref("int_finance__pricing") }} as pricing
            on booking.booking_id = pricing.bookingid
        left join
            clean_pricing_line2 on booking.booking_id = clean_pricing_line2.booking_id
        left join
            {{ ref("int_finance__cashflow_pricing") }} as cashflow_pricing
            on pricing.id = cashflow_pricing.pricingid
        left join
            {{ ref("int_finance__cashflow") }} as cashflow
            on cashflow_pricing.cashflowid = cashflow.id
        left join
            {{ ref("int_finance__cashflow_batch") }} as cashflow_batch
            on cashflow.batchid = cashflow_batch.id
        left join
            {{ ref("int_finance__invoice_cashflow") }} as invoice_cashflow
            on cashflow.id = invoice_cashflow.cashflow_id
        left join
            {{ ref("int_finance__invoice") }} as invoice
            on invoice_cashflow.invoice_id = invoice.invoice_id
        left join
            {{ source("raw", "applicative_database_invoice_line") }} as invoice_line
            on invoice.invoice_id = invoice_line.invoice_id
        where not invoice.invoice_reference like '%.2'
    ),

    coll_clean_pricing_line1 as (
        select
            collective_booking.collective_booking_id,
            case
                when category = 'offerer revenue' then pricing_line.amount
            end as offerer_revenue,
            case
                when category = 'offerer contribution' then pricing_line.amount
            end as offerer_contribution
        from {{ ref("mrt_global__collective_booking") }} as collective_booking
        left join
            {{ ref("int_finance__pricing") }} as pricing
            on collective_booking.collective_booking_id = pricing.collective_booking_id
        left join
            {{ ref("int_finance__pricing_line") }} as pricing_line
            on pricing.id = pricing_line.pricingid
    ),

    coll_clean_pricing_line2 as (
        select
            collective_booking_id,
            sum(- offerer_revenue / 100) as offerer_revenue,
            sum(offerer_contribution / 100) as offerer_contribution
        from coll_clean_pricing_line1
        group by 1
    ),

    collective as (
        select
            'collective' as booking_type,
            collective_booking.collective_booking_id,
            '' as booking_id,
            collective_booking.collective_booking_status,
            collective_booking.collective_booking_creation_date,
            collective_booking.collective_booking_cancellation_date,
            collective_booking.collective_booking_used_date,
            collective_booking.venue_id,
            collective_booking.offerer_id,
            collective_booking.educational_institution_id,
            pricing.id as pricing_id,
            pricing.status as pricing_status,
            pricing.creationdate as pricing_creation_date,
            - pricing.amount / 100 as pricing_amount,
            pricing.standardrule as pricing_rule,
            offerer_revenue,
            offerer_contribution,
            cashflow.id as cashflow_id,
            cashflow.creationdate as cashflow_creation_date,
            cashflow.status as cashflow_status,
            - cashflow.amount / 100 as cashflow_amount,
            cashflow_batch.creationdate as cashflow_batch_creation_date,
            cashflow_batch.label as cashflow_batch_label,
            invoice.invoice_id,
            invoice.invoice_reference,
            invoice.invoice_creation_date,
            invoice.amount as invoice_amount
        from
            {{ source("raw", "applicative_database_collective_booking") }}
            as collective_booking
        left join
            {{ ref("int_finance__pricing") }} as pricing
            on collective_booking.collective_booking_id = pricing.collective_booking_id
        left join
            coll_clean_pricing_line2
            on collective_booking.collective_booking_id
            = coll_clean_pricing_line2.collective_booking_id
        left join
            {{ ref("int_finance__cashflow_pricing") }} as cashflow_pricing
            on pricing.id = cashflow_pricing.pricingid
        left join
            {{ ref("int_finance__cashflow") }} as cashflow
            on cashflow_pricing.cashflowid = cashflow.id
        left join
            {{ ref("int_finance__cashflow_batch") }} as cashflow_batch
            on cashflow.batchid = cashflow_batch.id
        left join
            {{ ref("int_finance__invoice_cashflow") }} as invoice_cashflow
            on cashflow.id = invoice_cashflow.cashflow_id
        left join
            {{ ref("int_finance__invoice") }} as invoice
            on invoice_cashflow.invoice_id = invoice.invoice_id
        left join
            {{ source("raw", "applicative_database_invoice_line") }} as invoice_line
            on invoice.invoice_id = invoice_line.invoice_id
        where not invoice.invoice_reference like '%.2'
    )

select *
from individuel
union all
select *
from collective
