---
title: Collective Offer Domain
description: Description of the `mrt_global__collective_offer_domain` table.
---

{% docs description__mrt_global__collective_offer_domain %}

# Table: Collective Offer Domain

The `mrt_global__collective_offer_domain` table is designed to store the list of the educational domains associated to collective offers.

One row is one (collective offer, educational domain) pair: an offer can carry several domains, so counting offers requires `COUNT(DISTINCT collective_offer_id)` — a plain `COUNT(*)` counts pairs, not offers.

`collective_offer_id` holds ids from two different entities: the model is a `UNION ALL` of `collective_offer_domain` (real offers) and `collective_offer_template_domain`, whose `collective_offer_template_id` is aliased into the same column. A template is a reusable draft, not a bookable offer, so joining directly to `mrt_global__collective_offer` matches only part of the rows and silently drops the template ones — filter to the population you actually mean.

The referential is specific to the collective (EAC) side: there is no equivalent governed domain taxonomy for individual offers, so this table cannot answer "by cultural domain" questions about individual bookings.

{% enddocs %}

Collective offers can be related to several educational domain. The table is the corresponding table used to find the exhautive educational domain list of each collective offer.

## Table description

{% docs table__mrt_global__collective_offer_domain  %}{% enddocs %}
