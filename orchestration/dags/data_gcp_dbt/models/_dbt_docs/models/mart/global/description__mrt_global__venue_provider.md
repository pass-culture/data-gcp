---
title: Venue Provider
description: Description of the `mrt_global__venue_provider` table.
---

{% docs description__mrt_global__venue_provider %}

The `mrt_global__venue_provider` table lists all synchronizations established between cultural venues and synchronization instances (for ticketing, inventory management, etc.).

{% enddocs %}


All synchronizations, whether currently active or not, are displayed. A cultural venue (identified by a venue_id) can be linked to multiple synchronization instances, and a single synchronization instance can be set up for multiple cultural partners.

This table contains information about the cultural venue, the provider, and the synchronization link (e.g., setup date, last synchronization date, etc.).

## Table description

{% docs table__mrt_global__venue_provider %}{% enddocs %}
