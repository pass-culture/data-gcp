---
title: Offer - Item mapping
description: Description of the `int_applicative__offer_item_id` table.
---

{% docs description__int_applicative__offer_item_id %}

The `int_applicative__offer_item_id` table map offer_id to item_id.

The item_id is chosen with the following priority:

1. `product-<offer_product_id>` when the offer has a product;
2. `event-<event_series_id>` when the offer is linked to an event series (concerts, shows:
   all the dates and cities of the same tour share one item);
3. the item_linkage cluster id (`item_cluster_o<offer_id>`) from `ml_linkage.item_offer_mapping`;
4. `offer-<offer_id>` otherwise.

{% enddocs %}

## Table description

{% docs table__int_applicative__offer_item_id %}{% enddocs %}
