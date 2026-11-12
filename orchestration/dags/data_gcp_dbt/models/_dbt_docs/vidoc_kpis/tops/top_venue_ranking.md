---
title: Top venue rankings by geographical level
description: Monthly ranking of the top 50 partner cultural venues based on booking revenue at national, regional, and departmental levels.
---

## Top venue rankings by geographical level

### Definition

Identifies the top 50 partner cultural venues generating the most used booking revenue (`total_booking_amount`) per month across national, regional, and departmental scopes.

Ties in revenue are broken deterministically using `venue_id ASC`.

### Source table

[exp_vidoc__top_venue](../../models/export/vidoc/description__exp_vidoc__top_venue.md)

### Calculation

Query the top venues for a specific department and month.

```sql
SELECT
    partition_month,
    geo_level,
    geo_value,
    venue_rank,
    venue_id,
    venue_name,
    offerer_name,
    total_booking_amount
FROM `top_venue`
WHERE partition_month = '2026-10-01'
  AND geo_level = 'department'
  AND geo_value = 'Gironde'
ORDER BY venue_rank ASC
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Partition Month | `partition_month` | Truncated month of used bookings (`YYYY-MM-01`) |
| Geographic Level | `geo_level` | Resolution scope: `national`, `region`, or `department` |
| Geographic Value | `geo_value` | Name/code of the entity (`NAT`, region name, or department name) |
| Rank | `venue_rank` | Position from 1 to 50 based on total booking revenue |
| Venue ID | `venue_id` | Unique venue identifier |
| Venue Name | `venue_name` | Commercial venue name |
| Offerer Name | `offerer_name` | Legal entity or managing structure name |

### Temporal tracking

**Monthly.** Calculated on used bookings (`booking_is_used = TRUE`) aggregated by `booking_used_date` month.

### Data protection

Venues ranked beyond 50 are not exported.
