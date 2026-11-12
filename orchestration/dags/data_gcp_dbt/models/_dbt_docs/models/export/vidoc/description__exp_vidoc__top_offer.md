---
title: Top offer rankings for vidoc export
description: Key indicators from the `exp_vidoc__top_offer` model.
---

{% docs description__exp_vidoc__top_offer %}

The `exp_vidoc__top_offer` model provides the monthly ranking of the top 50 booked offers (items) at national, regional, and departmental levels.
It is designed to be exported to the Ministry for Vidoc visualization.

{% enddocs %}

## Table Description

Each row represents an item included in the top 50 most booked items for a given month and geographic area.

**Grain**: `partition_month`, `geo_level`, `geo_value`, `offer_rank`.

Item attributes (`offer_name`, `offer_category_id`, `offer_subcategory_id`) are consolidated to the canonical monthly version to avoid multi-vendor duplication.

{% docs table__exp_vidoc__top_offer %}{% enddocs %}
