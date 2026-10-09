---
title: Statistical Secret Cultural Partner Collective
description: Description of the `description__statistical_secret__cultural_partner_collective` table.
---

{% docs description__statistical_secret__cultural_partner_collective %}

The `description__statistical_secret__cultural_partner_collective` table provides a monthly evaluation of statistical secrecy compliance across departmental, EPCI, and municipal levels, based on a 6-month rolling window. This model computes evaluation of statistical secrecy based on the collective bookings only.

{% enddocs %}

## Table description

Each row represents the statistical secrecy status of a specific geography (municipality, EPCI, and department) for a given partition month.

Statistical secrecy is triggered at a geographic level if:
1. The average number of active partners over the last 6 months is 3 or fewer.
2. A single active partner accounts for more than 85% of the total intermediary revenue generated in the geography over the last 6 months.

This table details whether secrecy applies for each geographic granularity alongside the specific reason(s) for non-compliance.
