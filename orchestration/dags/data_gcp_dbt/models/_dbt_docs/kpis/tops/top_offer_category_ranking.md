---
title: Top offer rankings by category and geographic level
description: Monthly ranking of the top 50 booked offers within each offer category, by geographic level.
---

## Top offer rankings by category and geographic level

### Definition

Top 50 items ranked by used booking amount (`total_booking_amount`) **within each offer category**, per month across national (`NAT`), regional, and departmental perimeters. The `JEU` category is excluded.

Item metadata (`offer_name`, `offer_category_id`, `offer_subcategory_id`) are unified to the monthly most-booked version, guaranteeing a single consolidated row per item. Ties are resolved deterministically using `item_id ASC`.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc__top_offer_category](../../models/export/vidoc/description__exp_vidoc__top_offer_category.md)

### Calculation

Filter by month, geographic level, area name and category:

```sql
SELECT
    partition_month,
    geo_level,
    geo_value,
    offer_category_id,
    offer_rank,
    item_id,
    offer_name,
    total_booking_amount,
    total_booking_quantity
FROM `top_offer_category`
WHERE partition_month = '2026-10-01'
  AND geo_level = 'department'
  AND geo_value = 'Gironde'
  AND offer_category_id = 'LIVRE'
ORDER BY offer_rank ASC
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Partition Month | `partition_month` | Truncated month of used bookings (`YYYY-MM-01`) |
| Geographic Level | `geo_level` | Resolution scope: `national`, `region`, or `department` |
| Geographic Value | `geo_value` | Name of the entity (`NAT`, region name, or department name) |
| Category | `offer_category_id` | Offer category the ranking is computed in |
| Rank | `offer_rank` | Position from 1 to 50 within the category |
| Item | `item_id`, `offer_name` | Offer (grouped by item) and its monthly most-booked name |
| Subcategory | `offer_subcategory_id` | Offer subcategory |

### Temporal tracking

**Monthly.** Calculated on used bookings (`booking_is_used = TRUE`) aggregated by `booking_used_date` month.

### Data protection

Items ranked beyond 50 in their category are not exported.
