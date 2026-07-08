---
title: Metabase Asset Catalog
description: Description of the `int_metabase__asset_catalog` table.
---

{% docs description__int_metabase__asset_catalog %}
One row per Metabase asset (dashboard or card) with its placement in the collection
tree, the squad/tier/certified classification resolved by the metabase-governance
`taxonomy` job, usage signals, and (for dashboards) the markdown / member-card
context. The classified asset catalog used downstream for retrieval routing and
exposure selection.
{% enddocs %}

## Table description

{% docs table__int_metabase__asset_catalog %}{% enddocs %}
