---
title: Daily Offer Consultation
description: Description of the `mrt_native__daily_offer_consultation` table.
---

{% docs description__mrt_native__daily_offer_consultation %}

The `mrt_native__daily_offer_consultation` table aggregates offer-consultation events (`ConsultOffer`, `ConsultWholeOffer`, `ConsultDescriptionDetails`) by day, offer, traffic attribution, discovery module and user role/age, with `cnt_events` as the count of matching events.

It is used to analyse consultation volumes at the offer/offerer/venue level over time, broken down by acquisition channel (`origin`, `traffic_medium`, `traffic_campaign`), discovery module (`name`), and the consulting user's beneficiary status and age at the time of the event.

{% enddocs %}

## Table description
{% docs table__mrt_native__daily_offer_consultation %}{% enddocs %}
