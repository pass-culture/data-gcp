---
title: Finance daily available credits
description: Description of the `ml_finance__daily_available_credits` table.
---

{% docs description__ml_finance__daily_available_credits %}

# Table: Training Data Offer

The `ml_finance__daily_available_credits` table sums, for each day, the
**remaining (unspent) balance** of every individual pass Culture deposit
active that day: each deposit's `deposit_amount` minus its cumulative
non-cancelled booking spend as of that day (from `int_global__daily_deposit`
and `int_global__booking`), floored at 0 per deposit. It tracks the credit
mass actually available to beneficiaries at any point in time, including the
~March 2025 individual-credit reform (see `deposit_reform_category`), and is
used as an exogenous regressor for the `daily_pricing` Prophet model.

{% enddocs %}

## Table description

{% docs table__ml_finance__daily_available_credits %}{% enddocs %}
