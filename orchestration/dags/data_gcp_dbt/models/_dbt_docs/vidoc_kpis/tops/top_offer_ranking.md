---
title: Top offer rankings by geographic level
description: How to query the top 50 most booked offers by geographic resolution.
---

## Top offer rankings by geographic level

### Definition

Top 50 items ranked by used booking amount (`total_booking_amount`) per month across national (`NAT`), regional, and departmental perimeters.

Item metadata (`offer_name`, `offer_category_id`, `offer_subcategory_id`) are unified to the monthly most-booked version, guaranteeing a single consolidated row per item. Ties are resolved deterministically using `item_id ASC`.

### Source table

[exp_vidoc__top_offer](../../models/export/vidoc/description__exp_vidoc__top_offer.md)

### Calculation

Filter by month, geographic level, and area name:

```sql
SELECT
    partition_month,
    geo_level,
    geo_value,
    offer_rank,
    item_id,
    offer_name,
    offer_category_id,
    total_booking_amount,
    total_booking_quantity
FROM `top_offer`
WHERE partition_month = '2026-10-01'
  AND geo_level = 'region'
  AND geo_value = 'Auvergne-Rhône-Alpes'
ORDER BY offer_rank ASC
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Partition Month | `partition_month` | Truncated month of used bookings (`YYYY-MM-01`) |
| Geographic Level | `geo_level` | Resolution scope: `national`, `region`, or `department` |
| Geographic Value | `geo_value` | Name of the entity (`NAT`, region name, or department name) |
| Rank | `offer_rank` | Position from 1 to 50 based on total booking revenue |
| Item | `item_id`, `offer_name` | Offer (grouped by item) and its monthly most-booked name |
| Category | `offer_category_id`, `offer_subcategory_id` | Offer category and subcategory |

### Temporal tracking

**Monthly.** Calculated on used bookings (`booking_is_used = TRUE`) aggregated by `booking_used_date` month.

### Data protection

Items ranked beyond 50 are not exported.
