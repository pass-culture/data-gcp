---
title: Top 50 offers by category for vidoc export
description: Monthly top 50 offers (items) within each offer category by revenue, rank only.
---

{% docs description__exp_vidoc_top_offer_category %}

The `exp_vidoc__top_offer_category` model gives, for each month, territory and offer category, the 50 offers (items) with the highest revenue from used individual bookings. It is designed to be exported to the ministry for vidoc visualisation.

{% enddocs %}

## Table Description

Each row is one ranked entity for a month and a territory (national, region or department of the venue).

**Grain**: `partition_month`, `geo_level`, `region_name`, `department_code`, `offer_category_id`, `offer_rank`.

An offer of the top is an item (`item_id`): all the offers of the same work (same book, film, etc.), across all venues, are summed together. The item is grouped with its name, category and subcategory: when its offers differ on them (e.g. two editions named differently in two departments), the item can appear on several rows of a region or national top, each with part of its revenue. The `JEU` category is excluded, as in `external_reporting__top_offer_category`. The offer name is not available in this table.

The ranking is computed on the revenue of used bookings (`booking_intermediary_amount`) of the month, by venue location. Only the rank is exported: the revenue and the booked quantity behind it stay in the metrics model (no revenue at offer or venue level). Ties on revenue are not broken by any rule: the order of equal revenues, and which of them make the top 50 cut, can change from one run to the next. The ranking logic is the one of the matching `external_reporting__top_*` model.

`geo_level` is `national`, `region` or `department`. Region rows carry `region_code` and `region_name`; department rows carry `department_code`, `department_name` and their region; national rows have null geographic columns. Regions of the overseas collectivities (Saint-Martin, Saint-Pierre-et-Miquelon, Collectivités d'outre-mer) have a null `region_code`, as in the lot 1 tables.

In a department with few venues or offers, the top 50 can be close to the full list of what was booked that month.
