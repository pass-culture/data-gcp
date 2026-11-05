---
title: Top 50 venues for vidoc export
description: Monthly top 50 venues by revenue, rank only.
---

{% docs description__exp_vidoc_top_venue %}

The `exp_vidoc__top_venue` model gives, for each month and territory, the 50 venues with the highest revenue from used individual bookings. It is designed to be exported to the ministry for vidoc visualisation.

{% enddocs %}

## Table Description

Each row is one ranked entity for a month and a territory (national, region or department of the venue).

**Grain**: `partition_month`, `geo_level`, `region_name`, `department_code`, `venue_rank`.

A venue is ranked on all its used bookings of the month, whatever the offer category.

The ranking is computed on the revenue of used bookings (`booking_intermediary_amount`) of the month, by venue location. Only the rank is exported: the revenue and the booked quantity behind it stay in the metrics model (no revenue at offer or venue level). Ties on revenue are not broken by any rule: the order of equal revenues, and which of them make the top 50 cut, can change from one run to the next. The ranking logic is the one of the matching `external_reporting__top_*` model.

`geo_level` is `national`, `region` or `department`. Region rows carry `region_code` and `region_name`; department rows carry `department_code`, `department_name` and their region; national rows have null geographic columns. Regions of the overseas collectivities (Saint-Martin, Saint-Pierre-et-Miquelon, Collectivités d'outre-mer) have a null `region_code`, as in the lot 1 tables.

In a department with few venues or offers, the top 50 can be close to the full list of what was booked that month.
