---
title: Monthly Item Discoverability
description: Description of the `mrt_native__monthly_item_discoverability` table.
---

{% docs description__mrt_native__monthly_item_discoverability %}

The `mrt_native__monthly_item_discoverability` table is a one-row-per-item-per-month aggregate combining bookable-offer inventory (how many offers of an item were bookable, and on how many days) with how often that item was consulted, broken down by discovery channel (search, home, venue, favorites, similar-offer recommendations, other).

It is used to study how discoverable an item is relative to how much inventory of it is actually bookable, and through which channels users find it. An "item" is a generic content grouping used internally by the data team (`item_id`), shared across offer versions/providers of the same underlying content.

{% enddocs %}

## Table description
{% docs table__mrt_native__monthly_item_discoverability %}{% enddocs %}
