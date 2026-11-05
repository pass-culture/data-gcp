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

An offer of the top is an item (`item_id`): all the offers of the same work (same book, film, etc.), across all venues, are summed together. Its `offer_name`, `offer_category_id` and `offer_subcategory_id` are those of its most booked offers of the month. An item is ranked in its main category only. The `JEU` category is excluded.

The ranking is computed on the revenue of used bookings (`booking_intermediary_amount`) of the month, by venue location. Only the rank is exported: the revenue and the booked quantity behind it stay in the metrics model (no revenue at offer or venue level). Ties on revenue are broken by the entity columns in ascending order, so the rank and the top 50 cut are deterministic.

`geo_level` is `national`, `region` or `department`. Region rows carry `region_code` and `region_name`; department rows carry `department_code`, `department_name` and their region; national rows have null geographic columns. Regions of the overseas collectivities (Saint-Martin, Saint-Pierre-et-Miquelon, Collectivités d'outre-mer) have a null `region_code`, as in the lot 1 tables.

In a department with few venues or offers, the top 50 can be close to the full list of what was booked that month.
