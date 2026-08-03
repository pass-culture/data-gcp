---
title: Metabase Exposure Candidates
description: Description of the `int_metabase__exposure_candidates` table.
---

{% docs description__int_metabase__exposure_candidates %}
High-quality subset of `int_metabase__asset_catalog` selected for dbt exposure
generation: every in-scope, active, classified (tiered or certified) asset plus a
usage-based safety net. Replaces the legacy top-10-collections rule consumed by the
metabase-dbt export job.
{% enddocs %}

## Table description

{% docs table__int_metabase__exposure_candidates %}{% enddocs %}
