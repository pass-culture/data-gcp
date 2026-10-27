---
title: Finance weekly available credits
description: Description of the `ml_finance__weekly_available_credits` table.
---

{% docs description__ml_finance__weekly_available_credits %}

# Table: Training Data Offer

The `ml_finance__weekly_available_credits` table **averages**, per ISO week,
the daily remaining (unspent) credit balance from
`ml_finance__daily_available_credits`. It is a stock (a balance at a point in
time), not a flow, so it is averaged over the week rather than summed - unlike
`total_pricing`. It tracks the credit mass actually available to
beneficiaries at any point in time, including the ~March 2025
individual-credit reform (see `deposit_reform_category`), and is used as an
exogenous regressor for the `weekly_pricing` Prophet model.

{% enddocs %}

## Table description

{% docs table__ml_finance__weekly_available_credits %}{% enddocs %}
