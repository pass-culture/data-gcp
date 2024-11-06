---
title: Collective Offer
description: Description of the `mrt_global__collective_offer` table.
---

{% docs description__mrt_global__collective_offer %}

# Table: Collective Offer

The `mrt_global__collective_offer` table is designed to store comprehensive information about collecttives offers.

{% enddocs %}

Cultural partners can create two types of collective offers :
- collectives offers that can directly be booked by school teachers, with a set price and date. Those collective offers can be linked exclusively to a specific school (in this case, institution_id is not null). Those collective offers can be identified using the column collective_offer_is_template = 'False'.
- collective offer templates, with no set price and date. Those collective offers enable cultural partners to be visible towards school teachers, the latter being able to contact cultural partners to co-create a specific collective offer. Those collective offers can be identified using the column collective_offer_is_template = 'True'.

## Table description

{% docs table__mrt_global__collective_offer  %}{% enddocs %}
