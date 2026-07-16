The `int_finance__invoice_cashflow` table is a junction table linking invoices to cashflows.

## Business Context

This table allows tracing which cashflows are covered by each reimbursement receipt, enabling reconciliation between invoices and individual monetary flows.

## Table description

| name        | data_type | description                                               |
| ----------- | --------- | --------------------------------------------------------- |
| invoice_id  |           | Unique identifier for an invoice (reimbursement receipt). |
| cashflow_id |           | Unique identifier for a cashflow (monetary flow).         |
