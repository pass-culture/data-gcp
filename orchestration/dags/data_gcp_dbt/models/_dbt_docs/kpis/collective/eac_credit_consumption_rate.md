---
title: EAC credit consumption rate
description: How to calculate the share of the collective credit allocated to institutions that has been consumed.
---

## Share of the collective credit consumed by institutions

### Definition

Share of the credit allocated to the institutions (schools) for a school year that has been engaged in collective bookings since the start of that school year. The consumed credit is the amount of the confirmed, used or reimbursed bookings (cancelled and not yet confirmed bookings excluded), counted at the booking creation month. Same definition as `taux_d_utilisation_du_credit` in `external_reporting__eac_credit_usage`.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_institution_activity](../../models/export/vidoc/description__exp_vidoc_institution__activity.md)

### Calculation

The rate is the ratio of `cumulative_amount_spent` to `total_deposit`, on the latest month of each school year.

```sql
SELECT
    scholar_year,
    partition_month,
    SUM(cumulative_amount_spent) AS consumed_credit,
    SUM(total_deposit) AS allocated_credit,
    SAFE_DIVIDE(
        SUM(cumulative_amount_spent), SUM(total_deposit)
    ) AS credit_consumption_rate
FROM `institution_activity`
GROUP BY scholar_year, partition_month
QUALIFY partition_month = MAX(partition_month) OVER (PARTITION BY scholar_year)
ORDER BY scholar_year
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| School year | `scholar_year` | 1 September to 31 August |
| Month | `partition_month` | End of month situation |
| Department | `department_code`, `department_name` | Department of the institution |
| Academy | `academy_name` | Academy of the institution |
| Region | `region_code`, `region_name` | Region of the institution |

### Temporal tracking

**School year**, with one value per month: `cumulative_amount_spent` and `cumulative_reimbursed_amount` are cumulative since the start of the school year and `total_deposit` is the credit of the whole school year repeated on every month. Never sum over months.

### Notes and caveats

- During an ongoing school year the rate is partial: the credit is for the whole year, the consumption only up to the month.
- `cumulative_reimbursed_amount / total_deposit` gives the stricter rate of credit actually reimbursed (by month of use).
- Bookings of institutions without a known department are on a row with a null `department_code` and no credit: they count nationally but not in any territory.
- Amounts are the spending of all the schools of a territory, not the revenue of a partner: no statistical secret applies.
