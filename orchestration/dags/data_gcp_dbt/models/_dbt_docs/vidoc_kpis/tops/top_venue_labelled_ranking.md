---
title: Top labelled venue rankings by geographical level
description: Monthly ranking of the top 50 partner cultural venues holding Ministry of Culture labels or designations.
---

## Top labelled venue rankings by geographical level

### Definition

Identifies the top 50 partner cultural venues holding Ministry of Culture labels/designations (`Comptage partenaire label et appellation du MC`), ranked by used booking revenue per month across national, regional, and departmental scopes.

Ties in revenue are broken deterministically using `venue_id ASC`.

### Source table

[exp_vidoc__top_venue_labelled](../../models/export/vidoc/description__exp_vidoc__top_venue_labelled.md)

### Calculation

Query the top venues for a specific label and territory.

```sql
SELECT
    partition_month,
    geo_level,
    geo_value,
    venue_tag_name,
    venue_rank,
    venue_id,
    venue_name,
    offerer_name,
    total_booking_amount
FROM `exp_vidoc__top_venue_labelled`
WHERE partition_month = '2026-10-01'
  AND geo_level = 'national'
  AND geo_value = 'NAT'
  AND venue_tag_name = 'Scène nationale'
ORDER BY venue_rank ASC
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Partition Month | `partition_month` | Truncated month of used bookings (`YYYY-MM-01`) |
| Geographic Level | `geo_level` | Resolution scope: `national`, `region`, or `department` |
| Geographic Value | `geo_value` | Name/code of the entity (`NAT`, region name, or department name) |
| Venue Tag | `venue_tag_name` | Ministry of Culture designation/label name |
| Rank | `venue_rank` | Position from 1 to 50 within the tag partition |
| Venue ID | `venue_id` | Unique venue identifier |
| Venue Name | `venue_name` | Commercial venue name |
| Offerer Name | `offerer_name` | Legal entity or managing structure name |

### Temporal tracking

**Monthly.** Calculated on used bookings (`booking_is_used = TRUE`) aggregated by `booking_used_date` month.

### Data protection

Restricted to partners with verified ministry labels. Venues ranked beyond 50 are not exported.
