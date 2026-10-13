---
title: Booking Application Logs
description: Description of the `int_pcapi__booking_log` table.
---

{% docs description__int_pcapi__booking_log %}

# Table: Booking Application Logs

This model extracts backend API logs of booking and booking cancellation attempts made by young users from the app, to monitor the error rate of these critical features.

Each row is one attempt, identified by `booking_action` (the `<action>` of `native.bookings.<action>`, so new actions are picked up automatically). Success is derived from the log severity ('INFO' on success, 'WARNING' or above on failure).
- **book**: every booking attempt (`technical_message_id = 'native.bookings.book'`).
- **cancel**: failed cancellations (`native.bookings.cancel`, 'WARNING') and successful cancellations made by the young user (`booking.cancelled` logged by the native API with reason `BookingCancellationReasons.BENEFICIARY`).

Failures are classified in `failure_type` as 'business' (expected refusal, e.g. offer already booked) or 'technical' (provider API error, timeout, unknown exception).

Error rate = `countif(not is_success) / count(*)` per `booking_action`, optionally restricted to `failure_type = 'technical'`.

The full `extra` payload is kept in `extra_json`, so fields added later by the backend can be used without changing the model.

Logs are available from 2026-07-02.

{% enddocs %}

## Table description

{% docs table__int_pcapi__booking_log %}{% enddocs %}
