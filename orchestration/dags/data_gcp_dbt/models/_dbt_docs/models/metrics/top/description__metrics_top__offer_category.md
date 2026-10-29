---
title: Top offers by category and revenue
description: Monthly top 50 cultural offers by category ranked by booking revenue from the `metrics_top__offer_category` model.
---

{% docs description__metrics_top__offer_category %}

The `metrics_top__offer_category` model produces the monthly top 50 cultural offers within each offer category (excluding the "JEU" category), ranked by consumed booking revenue (`total_booking_amount`).
It enables category-specific market share analyses and trend detection across national, regional, and departmental territories.

{% enddocs %}

## Table Description

Each row represents a top-ranked cultural offer for a given month, offer category, geographic level (national, regional, or departmental), and geographic value, detailing its position, revenue, and volume.

{% docs table__metrics_top__offer_category %}{% enddocs %}
