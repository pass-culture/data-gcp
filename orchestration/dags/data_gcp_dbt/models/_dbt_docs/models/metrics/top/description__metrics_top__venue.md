---
title: Top venues by revenue
description: Monthly top 50 venues ranked by booking revenue across geographic levels from the `metrics_top__venue` model.
---

{% docs description__metrics_top__venue %}

The `metrics_top__venue` model provides the monthly ranking of the top 50 partner venues based on consumed booking revenue (`total_booking_amount`).
It is designed to power external reporting, governance monitoring, and Metabase exploration across multiple geographic scales (`national`, `region`, `department`).

{% enddocs %}

## Table Description

Each row represents a top-ranked venue for a given month, geographic level (national, regional, or departmental), and geographic value, along with its rank and cumulative booking revenue.

{% docs table__metrics_top__venue %}{% enddocs %}
