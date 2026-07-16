The `int_finance__pricing_log` table tracks the history of pricing status changes.

## Table description

| name         | data_type | description                                     |
| ------------ | --------- | ----------------------------------------------- |
| id           |           | Unique identifier for a pricing (valorisation). |
| pricingId    |           | Unique identifier for a pricing (valorisation). |
| timestamp    |           | Timestamp of the log entry.                     |
| statusBefore |           | Status before the change.                       |
| statusAfter  |           | Status after the change.                        |
| reason       |           | Reason for the log entry.                       |
