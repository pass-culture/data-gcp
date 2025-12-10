---
title: Venue Pricing Point Link
description: Description of the `int_finance__venue_pricing_point_link` table.
---

{% docs description__int_finance__venue_pricing_point_link %}

The `int_finance__venue_pricing_point_link` table links venues to their pricing points.

## Business Context

The **pricing point** determines how reimbursement amounts are calculated. The reimbursement rate can depend on the pricing point's cumulative revenue (tiered rates).

### Pricing Point Rules

- A venue **with SIRET** is automatically its own pricing point (no action required)
- A venue **without SIRET** must select a pricing point among its structure's venues that have a SIRET

### Temporal Aspect

This link is temporal (timespan): a venue can change its pricing point over time. The `pricing_point_link_beginning_date` and `pricing_point_link_ending_date` columns define the validity period.

### Important Note

Changing a venue's pricing point could reset its cumulative revenue, potentially allowing higher reimbursement rates. Such changes require accounting team validation if the current revenue exceeds 20,000€.

{% enddocs %}

## Table description

{% docs table__int_finance__venue_pricing_point_link %}{% enddocs %}
