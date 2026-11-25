## Used individual bookings and amount spent by category and beneficiary territory

### Definition

Number of used individual bookings made by beneficiaries (`total_used_bookings`) and amount they spent from their pass Culture credit (`total_used_amount`, in euros), by month of use, territory of **residence of the young person** and offer category. Categories are as declared by the cultural partner when creating the offer.

Unlike the [venue territory KPIs](https://pass-culture.github.io/data-gcp/dbt/kpis/partners/individual_bookings_venue_territory/index.md), bookings are counted when used, whether or not they are already in a payment batch.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_booking_beneficiary](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_booking_beneficiary/index.md)

### Calculation

Rows with `offer_category_id = 'ALL'` hold the territory total; the other rows the categories. Never sum the two together. Each level (`DEP`, `REG`, `GROUP`, `NAT`) has its own rows: filter on it, never sum departments.

The share of a category divides it by the `ALL` row of the same month and territory. Don't divide by the sum of the category rows: masked categories are NULL and the `ALL` row has its own mask, so that sum is not the territory total.

```
SELECT
    partition_month,
    offer_category_id,
    total_used_bookings AS used_bookings,
    total_used_amount AS amount_spent,
    SAFE_DIVIDE(
        total_used_bookings,
        MAX(IF(offer_category_id = 'ALL', total_used_bookings, NULL)) OVER (
            PARTITION BY partition_month, territory_level, territory_code
        )
    ) AS category_share
FROM `booking_beneficiary`
WHERE territory_level = 'NAT'
QUALIFY offer_category_id != 'ALL'
ORDER BY partition_month, used_bookings DESC
```

### Available dimensions

| Dimension     | Column                                                | Description                                                                                  |
| ------------- | ----------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| Month of use  | `partition_month`                                     | Month when the booking was used                                                              |
| Territory     | `territory_level`, `territory_code`, `territory_name` | Territory of residence, one level per row: `DEP`, `REG`, `GROUP` (METRO / DROM / COM), `NAT` |
| Parent region | `region_code`                                         | Region of a department (NULL for overseas collectivities)                                    |
| Category      | `offer_category_id`                                   | Offer category                                                                               |

### Temporal tracking

**Monthly.** Months from January 2022 to the month before the run date, as in external reporting. Monthly flows: sum the months for a period.

### Data protection

Low count rule, applied once in [exp_base\_\_booking_beneficiary](https://pass-culture.github.io/data-gcp/dbt/models/export/export_base/description__exp_base__booking_beneficiary/index.md). Values are not perturbed, but a row is NULL on every month (`is_low_count` TRUE) when fewer than 5 distinct beneficiaries account for it since 2022 (the bookers of the territory for an `ALL` row, of the category in the territory for a category row), or when the territory has fewer than 20 bookers for a category row. A secondary mask hides one more department when a region, group or France total would otherwise give a masked set of 1 to 4 bookers by subtraction. Each level is computed on unmasked data, so regions and France are exact.

### Notes and caveats

- The territory is the **current** address of the beneficiary, not his address at booking date: a young person who moved has all his past bookings in his new department.
- Bookings of users no longer known as beneficiaries (deleted, suspended, GRANT_FREE accounts: about 0.2% of the used bookings) have no residence and are left out, national total included. Beneficiaries without a located address are counted in France only.
- Category rows cover the external reporting categories (`get_categories()`); marginal categories are kept as is (`MEDIA`, `CARTE_JEUNES`: about 1,000-1,500 bookings a year each; `LIVRE` and `CINEMA` make up 85% of the bookings).
- Amounts are booking amounts paid with the credit: this is the young people's spending, not the partners' revenue.
