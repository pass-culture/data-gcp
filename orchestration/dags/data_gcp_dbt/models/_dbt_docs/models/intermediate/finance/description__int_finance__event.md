---
title: Finance Event
description: Description of the `int_finance__event` table.
---

{% docs description__int_finance__event %}

The `int_finance__event` table represents events or actions that could result in a monetary movement. This is the first step in the reimbursement lifecycle.

## Event Creation Triggers

A finance event is created when:
- A booking is marked as "used"
- A used booking is marked as "unused"
- A used booking is cancelled
- A finance incident (fraud, incorrect price, etc.) is validated

## Finance Event Status

| Status | Description |
|--------|-------------|
| `PENDING` | Waiting - the venue has no pricing point |
| `READY` | Priceable - the venue has a pricing point |
| `PRICED` | Valorised via the `price_events` cron |

## Ordering Date (`pricingOrderingDate`)

This field is crucial for ordering valorisations. It is calculated from:
- The booking usage date
- The stock date (for event offers)
- The venue-pricing point link date

Events must be valorised in chronological order to ensure reproducibility and correct revenue calculation for tiered reimbursement rules.

{% enddocs %}

## Table description

{% docs table__int_finance__event %}{% enddocs %}
