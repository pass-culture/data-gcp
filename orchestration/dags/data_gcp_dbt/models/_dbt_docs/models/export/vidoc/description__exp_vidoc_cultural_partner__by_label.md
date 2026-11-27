---
title: Cultural partner metrics by label for vidoc export
description: Key indicators from the `exp_vidoc_cultural_partner_by_label` model.
---

{% docs description__exp_vidoc_cultural_partner__by_label %}

The `exp_vidoc_cultural_partner_by_label` model provides the monthly number of active cultural partners (venues) by Ministry of Culture label, and of the partners holding at least one label (`partner_label = 'ALL'`).
It is designed to be exported to the ministry for vidoc visualisation (FI4.2 and FI4.3 on labelled partners). It sums the cells of [exp_base__cultural_partner_by_label](../export_base/description__exp_base__cultural_partner_by_label.md) to the department.

{% enddocs %}

## Table Description

Each row represents the active partners of one label for a month and a partner department (current venue location).

**Grain**: `partition_month`, `partner_region_code`, `partner_department_code`, `partner_label`.

> **Never sum this table over `partner_label`.** A partner can hold several labels. The `ALL` rows count the partners holding at least one label, once: use them for the labelled partners by channel (FI4.2). Summing over departments and regions for one label is correct.

- Labels are all the venue tags of category 1 ("Comptage partenaire label et appellation du MC"), not only the label groups of `exp_vidoc_cultural_partner_by_type` (`tag_*` rows): SMAC, for example, is here but in no `tag_*` row.
- Same activity rule (bookable offer in the last 365 days), months and `partner_region_code = '-1'` handling as `exp_vidoc_cultural_partner`. The cumulative activated partners are not exported to vidoc: they stay in `exp_base__cultural_partner_by_label`.

{% docs table__exp_vidoc__cultural_partner_by_label %}{% enddocs %}
