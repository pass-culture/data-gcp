---
title: Future Product Artist Link
description: Description of the `ml_linkage__future_product_artist_link` table.
---

{% docs description__ml_linkage__future_product_artist_link %}

# Table: Future Product Artist Link

The `ml_linkage__future_product_artist_link` table contains a preview of the product/artist link once ingestion of the delta product artist link and delta artist is completed.

This table is used to validate that the future state of the artists and product artist links are correct before synchronizing with the backend application by running dbt tests on it.

{% enddocs %}

## Table description

{% docs table__ml_linkage__future_product_artist_link %}{% enddocs %}
