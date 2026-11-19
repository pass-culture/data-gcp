---
title: Product
description: Description of the `mrt_global__product` table.
---

{% docs description__mrt_global__product %}

# Table: Product

The `mrt_global__product` table is the product catalog: one row per product, the reference record that
synchronized offers point to through `offer_product_id`. Products come from providers — Titelive for paper
books, CDs and vinyls (keyed by EAN), cinema providers for movies (keyed by Allociné id) — and several
offers, from different venues, can share one product.

The provider metadata (author, publisher, label, release date, price, GTL genre, movie runtime…) is
extracted from the product's JSON into columns; the raw JSON is not exposed. Offer and booking counts are
aggregated from the offers linked to the product.

{% enddocs %}

## Table description

{% docs table__mrt_global__product %}{% enddocs %}
