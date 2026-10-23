---
title: Individual Indicators
description: Key indicators from the `external_reporting__eac` model.
---

{% docs description__external_reporting__eac %}

The `external_reporting__eac` model provides aggregated key indicators related to pass Culture government priority politic for the collective part.
It is designed for automated export, especially in Excel format.

Collective offer and booking KPIs (`external_reporting__eac_offer_booking`) are a roll-up of the `metrics_offer__collective*`
and `metrics_booking__collective*` models. KPIs without suffix follow the venue (cultural partner) geography; KPIs
suffixed `_etablissements` follow the school geography. Use `aggregation` to combine months: sum `flow` KPIs, take
the last month of `stock` KPIs.

{% enddocs %}

## Table Description

Each row represents a key indicator calculated for a specific month and geographic aggregation level.
A lexique details the meaning of each kpi_name here: https://docs.google.com/spreadsheets/d/13Epof9MJbtN7srmDIld3-7QN0Hq6WpX7/edit?usp=sharing&ouid=113033507472186952579&rtpof=true&sd=true

{% docs table__external_reporting__eac %}{% enddocs %}
