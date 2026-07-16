The `int_finance__pricing` table represents the **valorisation** step in the reimbursement process. A pricing is created to calculate the reimbursement amount for a finance event (typically a used booking).

## Business Context

Only bookings marked as "used" (with a non-null `dateUsed`) are valorised. All used bookings are valorised, even if they are linked to non-reimbursable or free offers (the valorisation amount being zero in such cases).

## Pricing Status Lifecycle

| Status      | Description                                               |
| ----------- | --------------------------------------------------------- |
| `VALIDATED` | Pricing created via the `price_events` cron               |
| `PROCESSED` | Pricing included in a cashflow (payment batch generation) |
| `INVOICED`  | Reimbursement receipt generated                           |

## Amount Convention

All amounts are stored in **euro cents** (e.g., 12.34€ = 1234). By convention:

- **Negative amount**: Money paid by Pass Culture to the offerer
- **Positive amount**: Money owed by the offerer to Pass Culture (rare)

## Table description

| name                  | data_type | description                                                                                                    |
| --------------------- | --------- | -------------------------------------------------------------------------------------------------------------- |
| id                    |           | Unique identifier for a pricing (valorisation).                                                                |
| status                |           | Status of the pricing: VALIDATED (created), PROCESSED (included in cashflow), or INVOICED (invoice generated). |
| bookingId             |           | Unique identifier for a booking.                                                                               |
| collective_booking_id |           | Unique identifier for a collective booking.                                                                    |
| creationDate          |           | Date when the pricing was created.                                                                             |
| valueDate             |           | Date used for ordering the valorisation.                                                                       |
| amount                |           | Reimbursement amount in euro cents (negative = paid by Pass Culture to the offerer).                           |
| standardRule          |           | Standard reimbursement rule applied.                                                                           |
| customRuleId          |           | Identifier of the custom reimbursement rule if applicable.                                                     |
| revenue               |           | Cumulative revenue of the pricing point at the time of calculation.                                            |
| pricing_point_id      |           | Identifier of the pricing point (venue with SIRET).                                                            |
| venue_id              |           | Unique identifier for the venue.                                                                               |
| event_id              |           | Unique identifier for a finance event.                                                                         |
