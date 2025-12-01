---
title: Booking Finance Incident
description: Description of the `int_finance__booking_incident` table.
---

{% docs description__int_finance__booking_incident %}

The `int_finance__booking_incident` table represents the impact of a finance incident on specific bookings.

## Data Model

This table links to a parent `finance_incident` record (via `incident_id`) which contains:
- **kind**: Type of incident (fraud, incorrect price, etc.)
- **status**: Status of the incident
- **venue_id**: The venue concerned

The `booking_finance_incident` specifies which bookings are affected and the corrected amounts.

## Business Context

Finance incidents represent cases where the reimbursement of one or more bookings is incorrect. Motives include fraud, incorrect price, etc.

When a finance incident is validated, a finance event is created (linked via `finance_event.booking_finance_incident_id`) to account for the corresponding monetary movement (correction).

{% enddocs %}

## Table description

{% docs table__int_finance__booking_incident %}{% enddocs %}
