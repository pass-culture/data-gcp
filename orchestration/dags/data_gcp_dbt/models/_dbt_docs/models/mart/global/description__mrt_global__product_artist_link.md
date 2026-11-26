---
title: Product Artist Link
description: Description of the `mrt_global__product_artist_link` table.
---

{% docs description__mrt_global__product_artist_link %}

The `mrt_global__product_artist_link` table lists the artists of each product (books, music, movies), one row per (product, artist, role).

{% enddocs %}

## Table description

Every offer attached to a product (`mrt_global__offer.offer_product_id`) inherits the artists of the product. This is how most offers get their artists; artists entered directly on an offer are in `mrt_global__offer_artist_link`.
Every `artist_id` exists in `mrt_global__artist`.

{% docs table__mrt_global__product_artist_link %}{% enddocs %}
