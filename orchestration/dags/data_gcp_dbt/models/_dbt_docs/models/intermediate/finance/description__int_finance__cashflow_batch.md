---
title: Cashflow Batch
description: Description of the `int_finance__cashflow_batch` table.
---

{% docs description__int_finance__cashflow_batch %}

The `int_finance__cashflow_batch` table groups cashflows generated during the same reimbursement process execution.

## Business Context

Batches are generated on the 1st and 16th of each month. The **cutoff date** determines which valorisations are included in the batch:
- For early-month payments: cutoff is the last day of the previous month
- For mid-month payments: cutoff is the 15th of the current month

Only valorisations with a `valueDate` before the cutoff are included.

{% enddocs %}

## Table description

{% docs table__int_finance__cashflow_batch %}{% enddocs %}
