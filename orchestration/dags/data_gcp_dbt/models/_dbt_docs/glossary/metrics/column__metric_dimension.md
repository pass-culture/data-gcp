---
description: Description of the columns of the metrics aggregated models.
title: Metric dimensions
---


{% docs column__is_statistic_secret %}
A boolean flag indicating whether the data point is subject to statistical confidentiality (true) or not (false). This is triggered when the KPI falls below a minimum threshold to protect individual privacy and prevent re-identification.
{% enddocs %}

{% docs column__milestone_age %}
The age reached by the user at a specific key milestone, used to determine users eligibility.
{% enddocs %}

{% docs column__age_at_calculation %}
The user's age calculated at the specific reference date of the record (usually the end of the month). Unlike the current age, this value reflects the user's age at the historical point in time represented by the row.
{% enddocs %}

{% docs column__deposit_expiration_month %}
The month when the beneficiary's deposit expires.
{% enddocs %}
