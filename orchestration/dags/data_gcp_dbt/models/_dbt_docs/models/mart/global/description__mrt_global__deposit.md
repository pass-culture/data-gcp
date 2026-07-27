---
title: Deposit
description: Description of the `mrt_global__deposit` table.
---

{% docs description__mrt_global__deposit %}

The `Global Deposit` model offers a detailed view of deposit-related data by integrating key attributes to support analysis and reporting.

This model aggregates data on cultural credits (or financial deposits) allocated to users, based on their age, eligibility, location, and activity within the cultural application. It draws from both the deposit and user tables, providing a comprehensive view of each user's deposit details alongside demographic and engagement information.

*GRANT_15_17* refers to credits for users aged 15 to 17, capped at €80. This amount reflects specific age-based offers: €20 for users aged 15 and €30 for users aged 16 and 17.

*GRANT_18* covers credits for users aged 18, ranging between €300 and €500, with a duration of two years, offering an extended cultural allowance for this age group.

*Since the March 2025 reform*, GRANT_15_17 and GRANT_18 are being replaced, for new cohorts, by a single *GRANT_17_18* credit (150€, valid until the day before the beneficiary turns 21); 15–16-year-olds now receive *GRANT_FREE* (0€, granting access to free bookings only). GRANT_18 is therefore a legacy (pre-reform) credit: to measure conversion to an adult credit **across** the reform, count `GRANT_18` OR `GRANT_17_18` — never GRANT_18 alone, whose fall after March 2025 reflects the renaming, not a change in behaviour. Use `deposit_reform_category` to separate pre- and post-reform cohorts.

In this context, a "deposit" signifies a cultural credit allocated individually to users aged 15-18 through the application, granting access to cultural resources like books, digital goods, and various experiences. These deposits serve as financial allowances that differ by age and grant type (e.g., individual or duo offers), empowering users to engage with cultural activities either alone or with a companion.

{% enddocs %}


## Table description

{% docs table__mrt_global__deposit  %}{% enddocs %}
