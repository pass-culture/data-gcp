---
title: Offer
description: Description of the `exp_ministry__offer` table.
---

{% docs description__exp_ministry__offer %}

# Table: Offer

This model exports offers data for ministry use. It contains information about individual cultural offers including:
- Offer details (ID, name, description, category, subcategory)
- Offer characteristics (duo, bookable, digital/physical goods, event)
- Temporal information (creation, update, publication dates)
- Status information (synchronization, national scope, active status)
- Related entities (offerer, venue, product, item)
An offer in our model is a set product (identified via an offer_id) sold by a set cultural partner. For exemple, a set book (identified via its EAN) sold by a set cultural partner.

{% enddocs %}

## Table description

{% docs table__exp_ministry__offer %}{% enddocs %}
