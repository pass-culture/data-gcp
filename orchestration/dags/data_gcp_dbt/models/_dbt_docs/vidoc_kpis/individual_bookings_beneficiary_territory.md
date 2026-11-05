---
title: Individual bookings by category and beneficiary territory
description: How to calculate the number of used individual bookings and the amount spent by young people, by offer category and territory of residence.
---

## Used individual bookings and amount spent by category and beneficiary territory

### Definition

Number of used individual bookings made by beneficiaries (`total_used_bookings`) and amount they spent from their pass Culture credit (`total_used_amount`, in euros), by month of use, territory of **residence of the young person** and offer category. Categories are as declared by the cultural partner when creating the offer.

Unlike the [venue territory KPIs](individual_bookings_venue_territory.md), bookings are counted when used, whether or not they are already in a payment batch.

### Source table

[exp_vidoc_booking_beneficiary](../models/export/vidoc/description__exp_vidoc_booking_beneficiary.md)

### Calculation

Rows with `offer_category_id = 'ALL'` hold the department total; the other rows the categories. Never sum the two together.

```sql
SELECT
    t.partition_month,
    c.offer_category_id,
    SUM(c.total_used_bookings) AS used_bookings,
    SUM(c.total_used_amount) AS amount_spent,
    SAFE_DIVIDE(SUM(c.total_used_bookings), SUM(t.total_used_bookings)) AS category_share
FROM `booking_beneficiary` AS c
JOIN `booking_beneficiary` AS t
    ON t.partition_month = c.partition_month
    AND t.department_name = c.department_name
    AND t.offer_category_id = 'ALL'
WHERE c.offer_category_id != 'ALL'
    AND c.total_used_bookings IS NOT NULL
GROUP BY t.partition_month, c.offer_category_id
ORDER BY t.partition_month, used_bookings DESC
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Month of use | `partition_month` | Month when the booking was used |
| Department | `department_code`, `department_name` | Department of residence of the beneficiary |
| Region | `region_code`, `region_name` | Region of residence of the beneficiary |
| Category | `offer_category_id` | Offer category |

### Temporal tracking

**Monthly.** Months from January 2022 to the month before the run date, as in external reporting. Monthly flows: sum the months for a period.

### Data protection

Same low count rule as external reporting (`external_reporting__individual`, with `external_reporting__beneficiary_booking_bookers`). Values are not perturbed, but a row is NULL on every month (`is_low_count` TRUE) when fewer than 5 distinct beneficiaries account for it since 2022: the bookers of the department for an `ALL` row, the bookers of the category in the department for a category row. The category rows of a department with fewer than 20 bookers since 2022 are all NULL (no category breakdown for very small territories). Region and national totals summed from departments leave out the masked rows (rare: in August-October 2026 the `ALL` rows add up exactly to the national total).

### Notes and caveats

- The territory is the **current** address of the beneficiary, not his address at booking date: a young person who moved has all his past bookings in his new department.
- Bookings of users no longer known as beneficiaries (deleted, suspended, GRANT_FREE accounts: about 0.2% of the used bookings) have no residence and are left out, national total included. Beneficiaries without a located address are in department `-1` (`non localisé`).
- `region_code` is NULL for the overseas collectivities (Nouvelle-Calédonie, Saint-Martin, Polynésie Française, Saint-Pierre-et-Miquelon, Wallis et Futuna); `region_name` is filled.
- Category rows cover the external reporting categories (`get_categories()`); marginal categories are kept as is (`MEDIA`, `CARTE_JEUNES`: about 1,000-1,500 bookings a year each; `LIVRE` and `CINEMA` make up 85% of the bookings).
- Amounts are booking amounts paid with the credit: this is the young people's spending, not the partners' revenue.
