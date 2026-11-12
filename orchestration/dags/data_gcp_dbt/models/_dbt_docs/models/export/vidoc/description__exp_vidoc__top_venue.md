---
title: Top venue rankings for vidoc export
description: Key indicators from the `exp_vidoc__top_venue` model.
---

{% docs description__exp_vidoc__top_venue %}

The `exp_vidoc__top_venue` model provides the monthly ranking of the top 50 partner cultural venues based on booking revenue at national, regional, and departmental levels.
It is designed to be exported to the Ministry for Vidoc visualization.

{% enddocs %}

## Table Description

Each row represents a venue included in the top 50 venues by used booking amount for a given month and geographic area.

**Grain**: `partition_month`, `geo_level`, `geo_value`, `venue_rank`.

{% docs table__exp_vidoc__top_venue %}{% enddocs %}
