---
title: Cultural Partner Retention
description: Description of the `mrt_global__cultural_partner_retention` table.
---

{% docs description__mrt_global__cultural_partner_retention %}

The `mrt_global__cultural_partner_retention` table is a one-row-per-partner snapshot, companion to `mrt_global__cultural_partner`, that adds retention segmentation and engagement signals not carried on the base partner table: offer production, booking activity, revenue, consultations and favorites broken down over several time windows (last 2/6 months, and 2/6 months before the partner's last bookable offer), plus quality signals (fraud, rejected offers, ADAGE/DMS status, technical provider, reimbursement point).

Its main purpose is `partner_segmentation` — the `active` / `at_risk` / `churned` / `not-activated` retention segment of each partner, derived from how long ago the partner last had a bookable offer relative to sector-specific bookability-frequency thresholds.

A "partner" here is either a permanent venue or an offerer without a permanent venue (see `partner_status` on `mrt_global__cultural_partner`), matching the identifier used across the cultural-partner mart.

{% enddocs %}

## Table description
{% docs table__mrt_global__cultural_partner_retention %}{% enddocs %}
