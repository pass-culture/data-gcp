---
title: Offer
description: Description of the `exp_ministry__offer_removed` table.
---

{% docs description__exp_ministry__offer_removed %}

# Table: Offer

This model exports offers removed data for ministry use. It contains only removed offers not longer available for booking in pass Culture apps.
These offers are actually "useless" offers :
- They have never been booked
- They have never been bookable
- They haven't been modified in the year preceding their removal.

It contains information about individual cultural offers including:
- Offer details (ID, name, description, subcategory)
- Temporal information (creation, update, publication dates)
- Status information (synchronization, national scope, active status)
- Related entities (venue, product)

{% enddocs %}

## Table description

{% docs table__exp_ministry__offer_removed %}{% enddocs %}
