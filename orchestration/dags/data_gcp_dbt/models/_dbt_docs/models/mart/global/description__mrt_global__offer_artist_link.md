---
title: Offer Artist Link
description: Description of the `mrt_global__offer_artist_link` table.
---

{% docs description__mrt_global__offer_artist_link %}

The `mrt_global__offer_artist_link` table lists the artists attached directly to an offer, one row per link.

{% enddocs %}

## Table description

An offer gets its artists in two ways: through its product (`mrt_global__product_artist_link`, joined on `offer_product_id`), which covers most offers, or directly, entered by the partner on the offer itself (this table).
A direct link either points to an artist of `mrt_global__artist` (`artist_id`) or only carries the name the partner typed (`artist_name`, `artist_id` NULL).
To know whether an offer has at least one artist, look at both tables.

{% docs table__mrt_global__offer_artist_link %}{% enddocs %}
