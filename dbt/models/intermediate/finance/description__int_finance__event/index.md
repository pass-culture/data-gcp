The `int_finance__event` table represents events or actions that could result in a monetary movement. This is the first step in the reimbursement lifecycle.

## Event Creation Triggers

A finance event is created when:

- A booking is marked as "used"
- A used booking is marked as "unused"
- A used booking is cancelled
- A finance incident (fraud, incorrect price, etc.) is validated

## Finance Event Status

| Status    | Description                               |
| --------- | ----------------------------------------- |
| `PENDING` | Waiting - the venue has no pricing point  |
| `READY`   | Priceable - the venue has a pricing point |
| `PRICED`  | Valorised via the `price_events` cron     |

## Ordering Date (`pricingOrderingDate`)

This field is crucial for ordering valorisations. It is calculated from:

- The booking usage date
- The stock date (for event offers)
- The venue-pricing point link date

Events must be valorised in chronological order to ensure reproducibility and correct revenue calculation for tiered reimbursement rules.

## Table description

| name                              | data_type | description                                                                                                                                        |
| --------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| finance_event_id                  |           | Unique identifier for a finance event.                                                                                                             |
| finance_event_creation_date       |           | Date when the finance event was created.                                                                                                           |
| finance_event_value_date          |           | Value date of the finance event.                                                                                                                   |
| finance_event_price_ordering_date |           | Date used for ordering the valorisation of events. Calculated from booking usage date, stock date (for events), and venue-pricing point link date. |
| finance_event_status              |           | Status of the finance event: PENDING (no pricing point), READY (priceable), or PRICED (valorised).                                                 |
| finance_event_motive              |           | Motive of the finance event.                                                                                                                       |
| booking_id                        |           | Unique identifier for a booking.                                                                                                                   |
| collective_booking_id             |           | Unique identifier for a collective booking.                                                                                                        |
| venue_id                          |           | Unique identifier for the venue.                                                                                                                   |
| pricing_point_id                  |           | Identifier of the pricing point (venue with SIRET).                                                                                                |
| booking_finance_incident_id       |           | Unique identifier for a booking finance incident.                                                                                                  |
