The `int_finance__payment` table contains legacy payment data for bookings reimbursed **before January 1st, 2022**.

## Historical Context

This data model was effective for bookings validated before 01/01/2022. For more recent bookings, data is managed via the Pricing and Cashflow models.

A reimbursed booking is linked to a Payment. Pricings were created retrospectively and set to `invoiced` status for consistency, but there are no associated Cashflows.

**Note**: This model should only be used for historical analysis of pre-2022 reimbursements.

## Table description

| name                  | data_type | description                                        |
| --------------------- | --------- | -------------------------------------------------- |
| id                    |           | Unique identifier for a legacy payment (pre-2022). |
| author                |           | Author of the payment.                             |
| comment               |           | Comment associated with the payment.               |
| recipientName         |           | Name of the payment recipient.                     |
| bookingId             |           | Unique identifier for a booking.                   |
| amount                |           | Amount of the payment.                             |
| reimbursementRule     |           | Reimbursement rule applied.                        |
| transactionEndToEndId |           | End-to-end transaction identifier.                 |
| recipientSiren        |           | SIREN of the recipient.                            |
| reimbursementRate     |           | Reimbursement rate applied.                        |
| transactionLabel      |           | Label of the transaction.                          |
| paymentMessageId      |           | Identifier of the payment message.                 |
