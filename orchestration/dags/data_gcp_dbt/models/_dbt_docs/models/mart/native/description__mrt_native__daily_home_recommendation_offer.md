---
title: Native Daily Home Recommendation Offer
description: Description of the `mrt_native__daily_home_recommendation_offer` table.
---

{% docs description__mrt_native__daily_home_recommendation_offer %}

# Table: Daily Home Recommendation Offer

The `mrt_native__daily_home_recommendation_offer` table tracks offers displayed to users through recommendation modules on the native app home screen. For each recommendation call, it links the displayed offers with their associated user interactions (consultations, favorites, bookings), enabling analysis of recommendation performance and user engagement at the offer level.

This table is filtered to `playlist_origin = 'recommendation'` only. As a result, the effective values in this table are:
- `playlist_origin`: `recommendation`
- `context`: `recommendation:user_based`, `recommendation:tops`

{% enddocs %}


## Table description

{% docs table__mrt_native__daily_home_recommendation_offer %}{% enddocs %}
