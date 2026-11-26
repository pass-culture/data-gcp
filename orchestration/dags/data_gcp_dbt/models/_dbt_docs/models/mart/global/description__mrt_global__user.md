---
title: User
description: Description of the `mrt_global__user` table.
---

{% docs description__mrt_global__user %}

# User Model

The `mrt_global__user` table contains user-level information for all users in the system.

**Business Rules**

Each row represents a unique user.
All users are included in this table — general public, beneficiaries, and 15-16 — regardless of grant or credit status.
Pro, admin and anonymised accounts are not: an anonymised account leaves the table, while suspended and deleted accounts stay (`user_is_active` FALSE, with their `user_suspension_reason`).

{% enddocs %}

## Table description

{% docs table__mrt_global__user  %}{% enddocs %}
