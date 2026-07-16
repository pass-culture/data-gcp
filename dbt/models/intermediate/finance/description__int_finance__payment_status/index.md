The `int_finance__payment_status` table tracks the history of legacy payment status changes.

## Historical Context

This is a legacy model associated with the Payment model, used for bookings reimbursed **before January 1st, 2022**.

## Table description

| name      | data_type | description                                        |
| --------- | --------- | -------------------------------------------------- |
| id        |           | Unique identifier for the payment status record.   |
| paymentId |           | Unique identifier for a legacy payment (pre-2022). |
| date      |           | Date of the payment status change.                 |
| status    |           | Status of the payment.                             |
| detail    |           | Details about the status change.                   |
