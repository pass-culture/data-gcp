---
title: Cashflow Pricing Link
description: Description of the `int_finance__cashflow_pricing` table.
---

{% docs description__int_finance__cashflow_pricing %}

The `int_finance__cashflow_pricing` table is a junction table linking cashflows to pricings.

## Business Context

A cashflow can contain multiple pricings. This table allows tracing which valorisations are included in each monetary flow, enabling reconciliation between individual bookings and wire transfers.

{% enddocs %}

## Table description

{% docs table__int_finance__cashflow_pricing %}{% enddocs %}
