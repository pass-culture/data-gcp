## Top offer rankings for labelled partner venues

### Definition

Identifies the top 50 items generating the most used booking revenue (`total_booking_amount`) in venues tagged with partner labels and designations from the French Ministry of Culture (`venue_tag_category_label = 'Comptage partenaire label et appellation du MC'`).

Revenues and quantities are restricted to eligible venues, while item attributes (`offer_name`, `offer_category_id`, `offer_subcategory_id`) are mapped from the item's monthly canonical version.

Ties in revenue are broken deterministically using `item_id ASC`.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc\_\_top_offer_labelled](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc__top_offer_labelled/index.md)

### Calculation

Query the top offers for a given label and geographic area.

```
SELECT
    partition_month,
    geo_level,
    geo_value,
    venue_tag_name,
    offer_rank,
    item_id,
    offer_name,
    offer_category_id,
    offer_subcategory_id,
    total_booking_amount,
    total_booking_quantity
FROM `top_offer_labelled`
WHERE partition_month = '2026-10-01'
  AND geo_level = 'national'
  AND geo_value = 'NAT'
  AND venue_tag_name = 'Librairie indépendante de référence'
ORDER BY offer_rank ASC
```

### Available dimensions

| Dimension        | Column                 | Description                                                      |
| ---------------- | ---------------------- | ---------------------------------------------------------------- |
| Partition Month  | `partition_month`      | Truncated month of used bookings (`YYYY-MM-01`)                  |
| Geographic Level | `geo_level`            | Resolution scope: `national`, `region`, or `department`          |
| Geographic Value | `geo_value`            | Name/code of the entity (`NAT`, region name, or department name) |
| Venue Tag        | `venue_tag_name`       | Ministry of Culture partner designation/label                    |
| Rank             | `offer_rank`           | Position from 1 to 50 within the tag partition                   |
| Item ID          | `item_id`              | Canonical item identifier                                        |
| Offer Name       | `offer_name`           | Canonical title (most-booked variant across the month)           |
| Category         | `offer_category_id`    | Canonical macro-category identifier                              |
| Subcategory      | `offer_subcategory_id` | Canonical subcategory identifier                                 |

### Temporal tracking

**Monthly.** Calculated on used bookings (`booking_is_used = TRUE`) aggregated by `booking_used_date` month.

### Data protection

Restricted to partners with verified ministry labels. Truncated at rank 50.
