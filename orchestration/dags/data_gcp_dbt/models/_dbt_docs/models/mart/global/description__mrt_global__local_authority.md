---
title: Local Authority
description: Description of the `mrt_global__local_authority` table.
---

{% docs description__mrt_global__local_authority %}

The `mrt_global__local_authority` table captures data related to public administrations registered on pass Culture, including their types, activity status, and managed venues.

{% enddocs %}

All public administrations registered and validated on pass Culture, whether activated (at least one offer created) or not, are listed in this table.
Public administrations include all offerers (identified via an offerer_id) registered under NAF code (primary activity code) 84.11Z.
Each row corresponds to a public local authority offerer (one offerer_id).
This table is therefore a subset of the mrt_global__offerer table, and the fields related to activity correspond to those of the offerer.

## Table description

{% docs table__mrt_global__local_authority %}{% enddocs %}
