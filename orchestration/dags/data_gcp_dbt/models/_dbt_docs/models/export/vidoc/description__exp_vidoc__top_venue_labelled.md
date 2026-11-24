---
title: Top venue rankings for labelled partners for vidoc export
description: Key indicators from the `exp_vidoc__top_venue_labelled` model.
---

{% docs description__exp_vidoc__top_venue_labelled %}

The `exp_vidoc__top_venue_labelled` model provides the monthly ranking of the top 50 partner cultural venues holding Ministry of Culture labels/designations at national, regional, and departmental levels.
It is designed to be exported to the Ministry for Vidoc visualization.

{% enddocs %}

## Table Description

Each row represents a venue included in the top 50 venues within a partner label/designation for a given month and geographic area.

**Grain**: `partition_month`, `geo_level`, `geo_value`, `venue_tag_name`, `venue_rank`.

{% docs table__exp_vidoc__top_venue_labelled %}{% enddocs %}
