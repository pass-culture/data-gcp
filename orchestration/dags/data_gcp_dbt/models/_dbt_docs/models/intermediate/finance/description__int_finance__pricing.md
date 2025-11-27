---
title: Pricing (Valorisation)
description: Description of the `int_finance__pricing` table.
---

{% docs description__int_finance__pricing %}

The `int_finance__pricing` table represents the **valorisation** step in the reimbursement process. A pricing is created to calculate the reimbursement amount for a finance event (typically a used booking).

## Business Context

Only bookings marked as "used" (with a non-null `dateUsed`) are valorised. All used bookings are valorised, even if they are linked to non-reimbursable or free offers (the valorisation amount being zero in such cases).

## Pricing Status Lifecycle

| Status | Description |
|--------|-------------|
| `VALIDATED` | Pricing created via the `price_events` cron |
| `PROCESSED` | Pricing included in a cashflow (payment batch generation) |
| `INVOICED` | Reimbursement receipt generated |

## Amount Convention

All amounts are stored in **euro cents** (e.g., 12.34€ = 1234). By convention:
- **Negative amount**: Money paid by Pass Culture to the offerer
- **Positive amount**: Money owed by the offerer to Pass Culture (rare)

{% enddocs %}

## Table description

{% docs table__int_finance__pricing %}{% enddocs %}
