---
title: Monthly tops (offers and venues)
description: How the monthly top 50 offers, offers by category, venues, labelled venues and labelled offers are ranked.
---

## Monthly top 50 offers and venues

### Definition

For each month and territory, the 50 offers or venues with the highest revenue from used individual bookings, ranked from 1 to 50. Five tops are exported:

| Top | What is ranked | Source table |
|-----|----------------|--------------|
| Top offers | Offers (items), all categories | [exp_vidoc_top_offer](../models/export/vidoc/description__exp_vidoc_top_offer.md) |
| Top offers by category | Offers (items) within each offer category, `JEU` excluded | [exp_vidoc_top_offer_category](../models/export/vidoc/description__exp_vidoc_top_offer_category.md) |
| Top labelled offers | (item, label) pairs, bookings made in labelled venues only | [exp_vidoc_top_offer_labelled](../models/export/vidoc/description__exp_vidoc_top_offer_labelled.md) |
| Top venues | Venues | [exp_vidoc_top_venue](../models/export/vidoc/description__exp_vidoc_top_venue.md) |
| Top labelled venues | (venue, label) pairs | [exp_vidoc_top_venue_labelled](../models/export/vidoc/description__exp_vidoc_top_venue_labelled.md) |

Labelled venues hold a Ministry of Culture label or appellation (venue tag category 'Comptage partenaire label et appellation du MC', `venue_tag_name` gives the label).

**What an offer is.** An offer of the top is an **item** (`item_id`): all the offers of the same work (same book, film, etc.) are summed together, across all the venues of the territory. The item is grouped with its name, category and subcategory: when its offers differ on them (e.g. two editions named differently in two departments), the item can appear on several rows of a region or national top, each with part of its revenue. The top by category has no offer name.

The ranking rules are those of the `external_reporting__top_*` models (same source bookings, same `JEU` and label rules).

### Ranking rule

- **Ranked by revenue**: sum of `booking_intermediary_amount` of the bookings used in the month (`booking_used_date`), by venue location. There is no ranking by number of bookings.
- **Rank only**: the revenue and the booked quantity behind the rank are **not exported** (business secret: no revenue at offer or venue level).
- **Ties**: equal revenues get different ranks, with no tie-break rule: their order, and which of them make the top 50 cut, can change from one run to the next.
- **Labels**: the labelled tops rank all labels together (one top 50, not one per label). A venue with two labels appears twice, once per label.

### Calculation

The rank is already computed. Display a top by filtering one month, one territory (and one category or label when relevant), ordered by rank.

```sql
SELECT offer_rank, item_id, offer_name, offer_category_id
FROM `top_offer`
WHERE partition_month = '2026-09-01'
  AND geo_level = 'department'
  AND department_code = '48'
ORDER BY offer_rank
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Month | `partition_month` | Month when the bookings were used |
| Geographic level | `geo_level` | `national`, `region` or `department` |
| Region | `region_code`, `region_name` | Region of the venue (region and department rows) |
| Department | `department_code`, `department_name` | Department of the venue (department rows) |
| Category | `offer_category_id` | Top by category only |
| Label | `venue_tag_name` | Labelled tops only |

### Temporal tracking

**Monthly.** One top per `partition_month`. A month is complete once it is over; the current month, when present, only covers the bookings used so far.

### Notes and caveats

- Region codes of the overseas collectivities (Saint-Martin, Saint-Pierre-et-Miquelon, Collectivités d'outre-mer) are null, as in the other vidoc tables.
- **Small departments**: in a department with few venues or offers, the top 50 is close to the full list of what was booked in the month (a rank of 40 out of 45 booked venues carries little information).
- The overall top offers include the `JEU` category; only the top by category excludes it.
- The tops are a vidoc indicator only; they are not part of the observatoire.
