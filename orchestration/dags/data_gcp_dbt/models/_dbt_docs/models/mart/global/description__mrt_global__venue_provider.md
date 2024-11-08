---
title: Venue Provider
description: Description of the `mrt_global__venue_provider` table.
---

{% docs description__mrt_global__venue_provider %}

The `mrt_global__venue_provider` table lists the venues that automatically synchronize their offers on the app through a provider.

It contains information on:

- The cultural venue (venue_id, venue_name, venue_department_code, venue_creation_date, venue_is_permanent, venue_label)
- The provider (provider_id, provider_name, provider_is_active)
- The synchronized offers (total_individual_offers, total_collective_offers, first_individual_offer_creation_date, first_collective_offer_creation_date, last_sync_date)

{% enddocs %}

## Table description

{% docs table__mrt_global__venue_provider %}{% enddocs %}
