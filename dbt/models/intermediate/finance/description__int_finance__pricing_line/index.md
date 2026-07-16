The `int_finance__pricing_line` table details the components of a pricing (valorisation). Each pricing can be broken down into multiple lines by category.

## Categories

| Category               | Description                                                   |
| ---------------------- | ------------------------------------------------------------- |
| `offerer revenue`      | Main reimbursement amount paid by Pass Culture to the offerer |
| `offerer contribution` | Offerer's contribution/participation in the booking cost      |
| `commercial gesture`   | Special commercial arrangements (very rare)                   |

## Amount Convention

Like other finance amounts, values are in **euro cents** with sign convention:

- **Negative**: Money paid by Pass Culture to the offerer
- **Positive**: Money owed by the offerer to Pass Culture

## Table description

| name      | data_type | description                                                                                                                                                                                           |
| --------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| id        |           | Unique identifier for a pricing line.                                                                                                                                                                 |
| pricingId |           | Unique identifier for a pricing (valorisation).                                                                                                                                                       |
| amount    |           | Amount of this pricing line in euro cents.                                                                                                                                                            |
| category  |           | Category of the pricing line. Possible values: `offerer revenue` (main reimbursement to offerer), `offerer contribution` (offerer's participation), `commercial gesture` (rare special arrangements). |
