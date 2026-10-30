---
title: Top offers by revenue
description: Monthly top 50 cultural offers ranked by booking revenue from the `metrics_top__offer` model.
---

{% docs description__metrics_top__offer %}

The `metrics_top__offer` model delivers the monthly ranking of the top 50 individual offers (items) generating the highest booking revenue (`total_booking_amount`).
It also includes volume metrics (`total_booking_quantity`) and is broken down across geographic tiers (`national`, `region`, `department`) to identify key cultural products driving user engagement.

{% enddocs %}

## Table Description

Each row represents a top-ranked cultural offer for a given month, geographic level (national, regional, or departmental), and geographic value, including its revenue, booking volume, and ranking position.

{% docs table__metrics_top__offer %}{% enddocs %}
