The `int_finance__cashflow_pricing` table is a junction table linking cashflows to pricings.

## Business Context

A cashflow can contain multiple pricings. This table allows tracing which valorisations are included in each monetary flow, enabling reconciliation between individual bookings and wire transfers.

## Table description

| name       | data_type | description                                       |
| ---------- | --------- | ------------------------------------------------- |
| cashflowId |           | Unique identifier for a cashflow (monetary flow). |
| pricingId  |           | Unique identifier for a pricing (valorisation).   |
