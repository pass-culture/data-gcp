The `int_finance__cashflow_batch` table groups cashflows generated during the same reimbursement process execution.

## Business Context

Batches are generated on the 1st and 16th of each month. The **cutoff date** determines which valorisations are included in the batch:

- For early-month payments: cutoff is the last day of the previous month
- For mid-month payments: cutoff is the 15th of the current month

Only valorisations with a `valueDate` before the cutoff are included.

## Table description

| name         | data_type | description                                                                  |
| ------------ | --------- | ---------------------------------------------------------------------------- |
| id           |           | Identifier of the cashflow batch.                                            |
| creationDate |           | Date when the cashflow was created.                                          |
| cutoff       |           | Cutoff date - only valorisations before this date are included in the batch. |
| label        |           | Label description of the cashflow batch.                                     |
