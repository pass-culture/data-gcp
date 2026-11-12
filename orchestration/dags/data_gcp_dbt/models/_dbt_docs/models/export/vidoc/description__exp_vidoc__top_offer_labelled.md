---
title: Top offer rankings for labelled venues for vidoc export
description: Key indicators from the `exp_vidoc__top_offer_labelled` model.
---

{% docs description__exp_vidoc__top_offer_labelled %}

The `exp_vidoc__top_offer_labelled` model provides the monthly ranking of the top 50 booked offers within venues holding Ministry of Culture labels/designations at national, regional, and departmental levels.
It is designed to be exported to the Ministry for Vidoc visualization.

{% enddocs %}

## Table Description

Each row represents an item included in the top 50 most booked items for a given venue designation, month, and geographic area.

**Grain**: `partition_month`, `geo_level`, `geo_value`, `venue_tag_name`, `offer_rank`.

{% docs table__exp_vidoc__top_offer_labelled %}{% enddocs %}
