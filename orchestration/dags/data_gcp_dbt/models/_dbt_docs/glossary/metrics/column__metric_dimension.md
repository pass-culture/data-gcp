---
description: Description of the columns of the metrics aggregated models.
title: Metric dimensions
---


{% docs column__is_statistic_secret %}
A boolean flag indicating whether the data point is subject to statistical confidentiality (true) or not (false). This is triggered when the KPI falls below a minimum threshold to protect individual privacy and prevent re-identification.
{% enddocs %}

{% docs column__external_reporting_label %}
Display name of the territory: EPCI or municipality name for EPCI and COM, dimension_value otherwise.
{% enddocs %}

{% docs column__external_reporting_intitule %}
Display title of the territory, unique across levels so that it can be used alone as the territory filter: 'Name (code)' for EPCI and COM, 'Région Name' for REG, 'Académie de Name' (or "Académie d'Name") for ACAD, 'Rapport National' for NAT, dimension_value for DEP.
{% enddocs %}

{% docs column__external_reporting_is_secret_stat %}
True when the KPI is measured on the cultural partner (venue) geography and the territory (COM, EPCI or DEP) is subject to statistical secrecy at the last complete month, over the 6 or the 48-month rolling window (is_*_secret_stat of the statistical_secret__cultural_partner_* models). numerator, denominator and kpi are then NULL on every month. KPIs measured on the beneficiary or school geography are never secret, nor are REG, ACAD and NAT.
{% enddocs %}

{% docs column__external_reporting_is_low_count %}
True when the KPI is accounted for by 1 to 4 beneficiaries of the territory over the whole history (see external_reporting__beneficiary_booking_bookers): numerator, denominator and kpi are then NULL on every month. Also true on the months where an average or a share by person (montant_moyen_*, pct_beneficiaires_ayant_reserve_*) is computed over 1 to 4 beneficiaries. Beneficiary counts are blurred instead (cell key method) and never flagged.
{% enddocs %}

{% docs column__external_reporting_total_beneficiaries_behind_kpi %}
Number of distinct beneficiaries of the territory behind the KPI, from January 2022 to the last complete month.
{% enddocs %}

{% docs column__external_reporting_min_beneficiaries %}
Minimum number of distinct beneficiaries behind the KPI for it to be published: 5, or 20 for the category shares (pct_reservations_beneficiaires_*), which are published as a whole breakdown.
{% enddocs %}

{% docs column__external_reporting_secret_detail %}
Internal detail of the rule behind a masked KPI, not to be published: the statistical secret reason of the territory with its side and rolling window (e.g. 'Nombre de partenaires faible (< 3) (individuel, 6 mois)'), or the fewer than 5 beneficiaries rule. NULL when the KPI is not masked.
{% enddocs %}

{% docs column__external_reporting_is_national_only %}
True on the rows added for a KPI published at NAT but not at this territory level (e.g. coverage rates): numerator, denominator and kpi are NULL, only the national_* columns are filled, so that reports filtered on a territory still show the national value.
{% enddocs %}

{% docs column__external_reporting_national_numerator %}
Numerator of the same KPI and month at national level (NAT row), so that a report filtered on a territory can show the national figure from the same rows.
{% enddocs %}

{% docs column__external_reporting_national_denominator %}
Denominator of the same KPI and month at national level (NAT row).
{% enddocs %}

{% docs column__external_reporting_national_kpi %}
Value of the same KPI and month at national level (NAT row). NULL when the KPI is not published at NAT.
{% enddocs %}

{% docs column__external_reporting_secret_reason %}
Message to display instead of the KPI when it is masked, whatever the rule (statistical secret on cultural partners, or fewer than 5 beneficiaries behind the KPI): 'Données non disponibles (secret statistique)'. NULL otherwise. is_secret_stat and is_low_count tell the rule apart.
{% enddocs %}

{% docs column__milestone_age %}
Age milestone in whole years, used to determine users eligibility: months between birth month and the observation month divided by 12, kept when it is an integer, so milestone_age N is the cohort turning N that month. Stored as INT64 in beneficiary coverage models and in exp_vidoc__population_coverage, STRING in int_kpi__population_coverage and metrics_population__coverage.
{% enddocs %}

{% docs column__age_at_calculation %}
The user's age calculated at the specific reference date of the record (usually the end of the month). Unlike the current age, this value reflects the user's age at the historical point in time represented by the row.
{% enddocs %}

{% docs column__deposit_expiration_month %}
The month when the beneficiary's deposit expires.
{% enddocs %}

{% docs column__expired_cohorts__deposit_expiration_month %}
Month in which the beneficiaries' last deposit expired, as the first day of the month. Only closed months are present: deposits expiring in the current month are excluded.
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
True when the department is under statistical secrecy over the rolling window of the row (`rolling_window_months_count`): fewer than 3 active partners, top partner over 85% of the revenue, no revenue, or secondary secret (when only one department of France is under primary secret, the other department with the smallest revenue is masked too). True means its revenue must not be published.
{% enddocs %}

{% docs column__is_epci_secret %}
True when the EPCI is under statistical secrecy over the rolling window of the row (`rolling_window_months_count`): fewer than 3 active partners, top partner over 85% of the revenue, no revenue, or secondary secret (when only one EPCI of its department is under primary secret, the other EPCI with the smallest revenue is masked too). True means its revenue must not be published.
{% enddocs %}

{% docs column__is_municipality_secret %}
True when the municipality is under statistical secrecy over the rolling window of the row (`rolling_window_months_count`): fewer than 3 active partners, top partner over 85% of the revenue, no revenue, or secondary secret (when only one municipality of its EPCI is under primary secret, the other municipality with the smallest revenue is masked too). True means its revenue must not be published.
{% enddocs %}

{% docs column__has_department_activity %}
Indicates whether the department has activity over the rolling window (at least one active partner or a positive revenue). Territories without activity are not masked: their revenue is displayed as 0.
{% enddocs %}

{% docs column__is_department_secret_stat %}
Flag to use to mask financial data at the department level: true when the department has activity and is subject to primary or secondary statistical secrecy (has_department_activity and is_department_secret).
{% enddocs %}

{% docs column__has_epci_activity %}
Indicates whether the EPCI has activity over the rolling window (at least one active partner or a positive revenue). Territories without activity are not masked: their revenue is displayed as 0.
{% enddocs %}

{% docs column__is_epci_secret_stat %}
Flag to use to mask financial data at the EPCI level: true when the EPCI has activity and is subject to primary or secondary statistical secrecy (has_epci_activity and is_epci_secret).
{% enddocs %}

{% docs column__has_municipality_activity %}
Indicates whether the municipality has activity over the rolling window (at least one active partner or a positive revenue). Territories without activity are not masked: their revenue is displayed as 0.
{% enddocs %}

{% docs column__is_municipality_secret_stat %}
Flag to use to mask financial data at the municipality level: true when the municipality has activity and is subject to primary or secondary statistical secrecy (has_municipality_activity and is_municipality_secret).
{% enddocs %}

{% docs column__department_secret_reason %}
French display label of `department_secret_reason_code`. Stored values: `Pas de chiffre d'affaire`, `Nombre de partenaires faible (< 3)`, `Concentration forte du CA (> 85%)`, `Les deux raisons (< 3 partenaires ET > 85% CA)`, `Secret secondaire (masquage par soustraction)`, `Non soumis au secret`. Filter on the code, not on the label.
{% enddocs %}

{% docs column__epci_secret_reason %}
French display label of `epci_secret_reason_code`. Stored values: `Pas de chiffre d'affaire`, `Nombre de partenaires faible (< 3)`, `Concentration forte du CA (> 85%)`, `Les deux raisons (< 3 partenaires ET > 85% CA)`, `Secret secondaire (masquage par soustraction)`, `Non soumis au secret`. Filter on the code, not on the label.
{% enddocs %}

{% docs column__municipality_secret_reason %}
French display label of `municipality_secret_reason_code`. Stored values: `Pas de chiffre d'affaire`, `Nombre de partenaires faible (< 3)`, `Concentration forte du CA (> 85%)`, `Les deux raisons (< 3 partenaires ET > 85% CA)`, `Secret secondaire (masquage par soustraction)`, `Non soumis au secret`. Filter on the code, not on the label.
{% enddocs %}

{% docs column__rolling_window_months_count %}
Length in months of the rolling window ending with `partition_month` over which partners and revenue are counted (6 or 48). Every territory has one row per window: filter one value.
{% enddocs %}

{% docs column__total_revenue_amount_municipality %}
Total revenue generated by used bookings in the municipality over the rolling window.
{% enddocs %}

{% docs column__department_secret_reason_code %}
Stable code identifying the reason for statistical secrecy at the department level ('no_revenue', 'low_partners', 'high_concentration', 'both', 'secondary_secret', or 'none').
{% enddocs %}

{% docs column__epci_secret_reason_code %}
Stable code identifying the reason for statistical secrecy at the EPCI level ('no_revenue', 'low_partners', 'high_concentration', 'both', 'secondary_secret' or 'none').
{% enddocs %}

{% docs column__municipality_secret_reason_code %}
Stable code identifying the reason for statistical secrecy at the municipality level ('no_revenue', 'low_partners', 'high_concentration', 'both', 'secondary_secret' or 'none').
{% enddocs %}

{% docs column__geo_level %}
Geographic scale used to compute the ranking ('national', 'region', or 'department').
{% enddocs %}

{% docs column__geo_value %}
Name or identifier of the geographic area corresponding to the aggregation level ('NAT', region name, or department name).
{% enddocs %}

{% docs column__offer_rank %}
Rank position of the offer based on total booking revenue generated over the period, in descending order (from 1 to 50).
{% enddocs %}

{% docs column__venue_rank %}
Rank position of the venue based on total booking revenue generated over the period, in descending order (from 1 to 50).
{% enddocs %}
