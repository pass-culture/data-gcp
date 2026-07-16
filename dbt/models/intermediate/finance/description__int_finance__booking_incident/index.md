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

## Table description

| name                  | data_type | description                                       |
| --------------------- | --------- | ------------------------------------------------- |
| id                    |           | Unique identifier for a booking finance incident. |
| booking_id            |           | Unique identifier for a booking.                  |
| incident_id           |           | Identifier of the parent finance incident.        |
| beneficiary_id        |           | Unique identifier for a user.                     |
| collective_booking_id |           | Unique identifier for a collective booking.       |
| new_total_amount      |           | New total amount after incident correction.       |
