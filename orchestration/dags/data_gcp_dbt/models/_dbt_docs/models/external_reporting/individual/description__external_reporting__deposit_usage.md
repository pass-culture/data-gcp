---
title: Deposit Usage Indicators
description: Key indicators from the `external_reporting__deposit_usage` model.
---

{% docs description__external_reporting__deposit_usage %}

The `external_reporting__deposit_usage` model provides aggregated key indicators related to pass Culture deposit usage.
It is designed for automated export, especially in Excel format.

The `*_trimestre` KPIs are the same averages over the calendar quarter, on the row of its last month only: quarterly cohorts of small territories rarely count fewer than 5 beneficiaries, and quarters do not overlap so that a masked month cannot be deduced by difference.

{% enddocs %}

## Table Description

Each row represents a key indicator calculated for a specific month and geographic aggregation level.
A lexique details the meaning of each kpi_name here: https://docs.google.com/spreadsheets/d/13Epof9MJbtN7srmDIld3-7QN0Hq6WpX7/edit?usp=sharing&ouid=113033507472186952579&rtpof=true&sd=true

{% docs table__external_reporting__deposit_usage %}{% enddocs %}
