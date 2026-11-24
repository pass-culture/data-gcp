---
title: Top offer rankings by category for vidoc export
description: Key indicators from the `exp_vidoc__top_offer_category` model.
---

{% docs description__exp_vidoc__top_offer_category %}

The `exp_vidoc__top_offer_category` model provides monthly rankings of the top 50 booked offers within each offer category (excluding `JEU`) at national, regional, and departmental levels.
It is designed to be exported to the Ministry for Vidoc visualization.

{% enddocs %}

## Table Description

Each row represents an item included in the top 50 most booked items within a given category, month, and geographic area.

**Grain**: `partition_month`, `geo_level`, `geo_value`, `offer_category_id`, `offer_rank`.

{% docs table__exp_vidoc__top_offer_category %}{% enddocs %}
