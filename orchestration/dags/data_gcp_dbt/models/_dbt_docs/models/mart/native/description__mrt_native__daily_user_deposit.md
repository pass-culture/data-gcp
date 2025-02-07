---
title: Population User Deposits
description: Description of the `population_user_deposits` table.
---

{% docs description__mrt_native__daily_user_deposit %}
This table records deposit activity for users over time, including user details and transaction information.

### **Business Rules**
- The table is **incrementally updated** using an **insert-overwrite** strategy.
- It tracks deposits **between** their creation date and expiration date.
- User age is calculated dynamically based on the transaction date.

{% enddocs %}

## Table description

{% docs table__mrt_native__daily_user_deposit %}{% enddocs %}
