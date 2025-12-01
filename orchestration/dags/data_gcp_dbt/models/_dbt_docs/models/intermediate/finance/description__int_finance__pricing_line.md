---
title: Pricing Line
description: Description of the `int_finance__pricing_line` table.
---

{% docs description__int_finance__pricing_line %}

The `int_finance__pricing_line` table details the components of a pricing (valorisation). Each pricing can be broken down into multiple lines by category.

## Categories

| Category | Description |
|----------|-------------|
| `offerer revenue` | Main reimbursement amount paid by Pass Culture to the offerer |
| `offerer contribution` | Offerer's contribution/participation in the booking cost |
| `commercial gesture` | Special commercial arrangements (very rare) |

## Amount Convention

Like other finance amounts, values are in **euro cents** with sign convention:
- **Negative**: Money paid by Pass Culture to the offerer
- **Positive**: Money owed by the offerer to Pass Culture

{% enddocs %}

## Table description

{% docs table__int_finance__pricing_line %}{% enddocs %}
