---
title: Number of active and activated cultural partners by type
description: How to calculate the number of active and cumulatively activated cultural partners for a given partner type.
---

## Number of active and activated cultural partners by type

### Definition

Same definitions as [active cultural partners](active_cultural_partners.md) and [activated cultural partners](activated_cultural_partners.md), restricted to the partners of one **partner type** (cinemas, bookshops, live shows, museums, festivals, ...).

Available columns: active partners `total_active_partners_{global, individual, collective, dual_part}` and cumulative activated partners `total_cumulative_activated_partners_{global, individual, collective, dual_part, individual_only, collective_only}`. The individual only / collective only split of active partners is not available by type.

Types and labels are defined in the `get_partner_types` macro, from the venue type and venue tags. Label rows (`tag_*`: art et essai cinemas, LIR bookshops, labelled live shows, labelled museums) are subsets of a type.

### Source table

[exp_vidoc_cultural_partner_by_type](../models/export/vidoc/description__exp_vidoc_cultural_partner__by_type.md). `total_active_partners_individual` is the same KPI as `external_reporting__cultural_partner` (`nombre_de_partenaire_actif_individuel_<type>`).

### Calculation

Filter on **one** type, then sum over the geography.

```sql
SELECT
    partition_month,
    partner_type,
    SUM(total_active_partners_global) AS active_partners,
    SUM(total_cumulative_activated_partners_global) AS activated_partners
FROM `cultural_partner_by_type`
GROUP BY partition_month, partner_type
ORDER BY partition_month, partner_type
```

> **Never sum over `partner_type`.** A partner can belong to several types: `Festival` venues are in both `musique_live` and `festival`, a venue tagged "Cinéma d'art et d'essai" is in `cinemas` and in the type of its venue, and `tag_*` rows are subsets of a type. Partners whose type is not listed are in no type. Take totals from [exp_vidoc_cultural_partner](../models/export/vidoc/description__exp_vidoc_cultural_partner.md).

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Month | `partition_month` | Observation month |
| Partner type | `partner_type` | Type or label name from `get_partner_types` |
| Department | `partner_department_code`, `partner_department_name` | Department of the venue |
| Region | `partner_region_code`, `partner_region_name` | Region of the venue |

### Notes and caveats

- The partner type and the location are the **current** ones, applied to all past months.
- Same months, same `-1` region handling and same data protection status (not perturbed, no secrecy) as [active cultural partners](active_cultural_partners.md).
