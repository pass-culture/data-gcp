---
title: User Offerer
description: Description of the `mrt_global__user_offerer` table.
hide: "true"
---

{% docs description__mrt_global__user_offerer %}

# Table: Global Offerer

The `mrt_global__user_offerer` table provides a detailed view of user accounts affiliated to or managing a cultural offerer (identified by an offerer_id). We thus have one row per user and offerer affiliation.
Those accounts enable them to manage the said offerer (by creating and updating offers, managing bookings etc).

{% enddocs %}

Multiple users can be affiliated to the same offerer (for example, when a cultural offerer has various staff members) and a single user can be affiliated to multiple offerers.
Only validated user - offerer affiliations are stored in this model (pending / rejected affiliations are excluded).


## Table description

{% docs table__mrt_global__user_offerer  %}{% enddocs %}
