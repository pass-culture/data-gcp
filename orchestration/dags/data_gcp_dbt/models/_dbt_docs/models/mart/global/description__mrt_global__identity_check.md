---
title: Identity check
description: Description of the `mrt_global__identity_check` table.
---

{% docs description__mrt_global__identity_check %}

# Identity check Model

The `mrt_global__identity_check` table lists the identity checks young users go through before receiving a credit, one row per check, with its provider (Ubble, Démarches Simplifiées, EduConnect, Jouve), its current status and its creation and last update dates.

**Business Rules**

- Each row is one attempt: a user can have several checks, with one or several providers, until one is validated (status OK).
- Only identity checks are included. The other backend checks (profile completion, honor statement, phone validation, user profiling, bonus credit eligibility) are not, and neither are free-text reasons nor provider payloads.
- The credit is unlocked after a validated check: the deposit date (`mrt_global__deposit.deposit_creation_date`) comes after the check, not at its creation.

{% enddocs %}

## Table description

{% docs table__mrt_global__identity_check  %}{% enddocs %}
