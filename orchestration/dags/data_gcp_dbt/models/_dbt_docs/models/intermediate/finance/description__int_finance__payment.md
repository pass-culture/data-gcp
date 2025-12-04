---
title: Payment (Legacy)
description: Description of the `int_finance__payment` table.
---

{% docs description__int_finance__payment %}

The `int_finance__payment` table contains legacy payment data for bookings reimbursed **before January 1st, 2022**.

## Historical Context

This data model was effective for bookings validated before 01/01/2022. For more recent bookings, data is managed via the Pricing and Cashflow models.

A reimbursed booking is linked to a Payment. Pricings were created retrospectively and set to `invoiced` status for consistency, but there are no associated Cashflows.

**Note**: This model should only be used for historical analysis of pre-2022 reimbursements.

{% enddocs %}

## Table description

{% docs table__int_finance__payment %}{% enddocs %}
