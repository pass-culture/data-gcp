---
title: Deposit
description: Description of the `mrt_global__deposit` table.
---

{% docs description__mrt_global__deposit %}

The `Global Deposit` model offers a detailed view of deposit-related data by integrating key attributes to support analysis and reporting.

This model aggregates data on cultural credits (or financial deposits) allocated to users, based on their age, eligibility, location, and activity within the cultural application. It draws from both the deposit and user tables, providing a comprehensive view of each user's deposit details alongside demographic and engagement information.

*GRANT_15_17* refers to credits for users aged 15 to 17, capped at €80. This amount reflects specific age-based offers: €20 for users aged 15 and €30 for users aged 16 and 17.

*GRANT_18* covers credits for users aged 18, ranging between €300 and €500, with a duration of two years, offering an extended cultural allowance for this age group.

In this context, a "deposit" signifies a cultural credit allocated individually to users aged 15-18 through the application, granting access to cultural resources like books, digital goods, and various experiences. These deposits serve as financial allowances that differ by age and grant type (e.g., individual or duo offers), empowering users to engage with cultural activities either alone or with a companion.

{% enddocs %}


## Table description

{% docs table__mrt_global__deposit  %}{% enddocs %}

