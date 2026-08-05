---
title: Booking Reimbursement
description: Description of the `mrt_finance__booking_reimbursement` table.
---

{% docs description__mrt_finance__booking_reimbursement %}

The `mrt_finance__booking_reimbursement` table unions individual and collective bookings with their full finance reconciliation chain: pricing (valorisation), cashflow and invoice. It is a finance-facing table used to reconcile what a booking generated for the offerer against what was actually paid out and invoiced, at the individual booking grain.

Each row is a booking (individual or collective, identified by `booking_type`) joined to at most one pricing, one cashflow and one invoice; the `booking_id` / `collective_booking_id` and `educational_institution_id` columns are populated only for the row type they apply to (empty string on the other type). `offerer_revenue` and `offerer_contribution` are the two components of `pricing_amount` (per the reimbursement rule's `offerer revenue` / `offerer contribution` pricing-line categories).

Rows are restricted to bookings that reached at least the pricing step; a booking with no pricing yet (not priceable) will not appear here.

{% enddocs %}

## Table description
{% docs table__mrt_finance__booking_reimbursement %}{% enddocs %}
