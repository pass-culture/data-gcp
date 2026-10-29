---
title: EAC format Indicators
description: Key indicators from the `external_reporting__eac_format` model.
---

{% docs description__external_reporting__eac_format %}

The `external_reporting__eac_format` model provides collective bookings, amounts and tickets split by offer format (institution geography), rolled up from `metrics_booking__collective_by_format`.
KPI names are `total_reservations_format_<format>`, `total_montant_depense_format_<format>` and `total_tickets_generes_format_<format>`, monthly flows. An offer with several formats counts in each of them.

{% enddocs %}

## Table Description

Each row represents a key indicator calculated for a specific month and geographic aggregation level.

{% docs table__external_reporting__eac_format %}{% enddocs %}
