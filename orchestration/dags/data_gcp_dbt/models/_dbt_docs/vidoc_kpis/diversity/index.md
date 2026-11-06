---
title: "Vidoc KPIs: Diversity"
description: "Diversity of the cultural practices of young people at credit expiration: number of categories booked, categories and distinct venues."
---

## Diversity

Diversity of the cultural practices of young people at credit expiration: number of categories booked, categories and distinct venues.

**Data protection**: Counts are perturbed (cell key method); the number of distinct venues is hidden when fewer than 5 beneficiaries are in the row. See [Statistical confidentiality](../../references/statistical_confidentiality.md).

| KPI | Definition | Source table(s) | Tracking |
|-----|-----------|-----------------|----------|
| [Diversity rate (3+ categories)](diversity_3plus_categories.md) | Share who booked in 3+ categories at credit expiration | `exp_vidoc_diversity` | Monthly |
| [Diversity depth (1+ to 5+ categories)](diversity_category_depth.md) | Share who booked in at least N categories at credit expiration, N = 1 to 5 | `exp_vidoc_diversity` | Monthly |
| [Booking share by category](booking_share_by_category.md) | Share who booked in category X at credit expiration | `exp_vidoc_diversity_by_category` + `exp_vidoc_diversity` | Monthly |
| [Distinct venues booked](distinct_venues_booked.md) | Average number of distinct venues booked per beneficiary at expiration | `exp_vidoc_diversity` | Monthly |
