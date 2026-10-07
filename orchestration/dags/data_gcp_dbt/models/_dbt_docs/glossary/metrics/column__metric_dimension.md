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

{% docs column__activity_month %}
The month during which the connection and engagement metrics are measured (format: YYYY-MM-01).
{% enddocs %}

{% docs column__signup_week %}
The start date of the week during which the beneficiary initiated their registration or onboarding process (format: YYYY-MM-DD, starting on Monday).
{% enddocs %}

{% docs column__age_at_signup %}
The age reached by the beneficiary at the time of signup/onboarding initiation.
{% enddocs %}

{% docs column__is_department_secret %}
Indicates whether the department is subject to statistical secrecy over the 6-month rolling window (true if active partners <= 3 or single partner revenue share > 85%).
{% enddocs %}

{% docs column__is_epci_secret %}
Indicates whether the EPCI is subject to statistical secrecy over the 6-month rolling window (true if active partners <= 3 or single partner revenue share > 85%).
{% enddocs %}

{% docs column__is_municipality_secret %}
Indicates whether the municipality is subject to statistical secrecy over the 6-month rolling window (true if active partners <= 3 or single partner revenue share > 85%).
{% enddocs %}

{% docs column__department_secret_reason %}
Specific reason for statistical secrecy at the department level ('Nombre de partenaires faible (<= 3)', 'Concentration forte du CA (> 85%)', 'Les deux raisons (<= 3 partenaires ET > 85% CA)', or 'Non soumis au secret').
{% enddocs %}

{% docs column__epci_secret_reason %}
Specific reason for statistical secrecy at the EPCI level ('Nombre de partenaires faible (<= 3)', 'Concentration forte du CA (> 85%)', 'Les deux raisons (<= 3 partenaires ET > 85% CA)', or 'Non soumis au secret').
{% enddocs %}

{% docs column__municipality_secret_reason %}
Specific reason for statistical secrecy at the municipality level ('Nombre de partenaires faible (<= 3)', 'Concentration forte du CA (> 85%)', 'Les deux raisons (<= 3 partenaires ET > 85% CA)', or 'Non soumis au secret').
{% enddocs %}
