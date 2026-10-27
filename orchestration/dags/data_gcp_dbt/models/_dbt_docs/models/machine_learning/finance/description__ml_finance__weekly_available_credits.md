---
title: Finance weekly available credits
description: Description of the `ml_finance__weekly_available_credits` table.
---

{% docs description__ml_finance__weekly_available_credits %}

# Table: Training Data Offer

The `ml_finance__weekly_available_credits` table sums, per ISO week, the
`deposit_amount` of every individual pass Culture deposit active that week
(from `int_global__daily_deposit`). It tracks the total granted credit mass
outstanding at any point in time, including the ~March 2025 individual-credit
reform (see `deposit_reform_category`), and is used as an exogenous regressor
for the `weekly_pricing` Prophet model.

{% enddocs %}

## Table description

{% docs table__ml_finance__weekly_available_credits %}{% enddocs %}
