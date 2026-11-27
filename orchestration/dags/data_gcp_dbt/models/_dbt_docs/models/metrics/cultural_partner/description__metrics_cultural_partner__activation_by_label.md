---
title: Cultural Partners activation by label metrics
description: Key indicators from the `metrics_cultural_partner__activation_by_label` model.
---

{% docs description__metrics_cultural_partner__activation_by_label %}

The `metrics_cultural_partner__activation_by_label` model provides the monthly active and cumulative activated cultural partners (venues) by Ministry of Culture label (venue tags of category 1), with the same rules as `metrics_cultural_partner__activation_by_type`.

{% enddocs %}

## Table Description

Each row represents the active and activated partners of one label (`partner_label`) for a month and a venue geography. `partner_label = 'ALL'` counts the partners holding at least one label, once. A partner can hold several labels: labels are not additive, use the `ALL` rows for totals.

{% docs table__metrics_cultural_partner__activation_by_label %}{% enddocs %}
