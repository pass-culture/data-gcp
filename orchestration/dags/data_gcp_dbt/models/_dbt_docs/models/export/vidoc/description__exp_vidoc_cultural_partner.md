---
title: Cultural partner metrics for vidoc export
description: Key indicators from the `exp_vidoc_cultural_partner` model.
---

{% docs description__exp_vidoc_cultural_partner %}

The `exp_vidoc_cultural_partner` model provides the monthly number of active and cumulatively activated cultural partners (venues), by channel (individual, collective, both).
It is designed to be exported to the ministry for vidoc visualisation. It uses the same metrics table (`metrics_cultural_partner__activation`) and definitions as `external_reporting__cultural_partner`.

{% enddocs %}

## Table Description

Each row represents the cultural partner indicators for a month and a partner department (the venue's current location).

**Grain**: `partition_month`, `partner_region_code`, `partner_department_code`.

- Each partner is counted once per row, so this table can be summed over departments and regions. Use it for totals, never `exp_vidoc_cultural_partner_by_type`.
- Channel split: `individual = individual_only + dual_part`, `collective = collective_only + dual_part`, `global = individual_only + collective_only + dual_part` (also for the cumulative columns).
- All months from `2022-01` are present, as in external_reporting. The bookable offer history starts on 2022-06-10: months before 2022-06 are zero and the 365-day window is truncated until 2023-05. The current month is computed up to today and changes until the month is closed.
- Overseas collectivities outside any region (Saint-Pierre-et-Miquelon `975`, Saint-Martin `978`, Nouvelle-Calédonie `988`) and non-located partners (`980`) have `partner_region_code = '-1'`: they are counted in the national total but in no region.
- Counts are not perturbed and not under statistical secrecy: partner lists are public.

{% docs table__exp_vidoc__cultural_partner %}{% enddocs %}
