---
title: Vidoc KPIs
description: Key performance indicators exported for the ministry's vidoc visualisation tool.
---

## Vidoc KPIs

This section documents the key performance indicators (KPIs) exported for the ministry via the **vidoc** export tables. For each indicator you will find the definition, source table(s), SQL calculation rule, and available dimensions.

All vidoc export tables are documented individually in the **Table definitions** section under `export > vidoc`.

### Data protection

All exported counts are protected using the **Cell Key Perturbation method** — the current standard recommended by INSEE and Eurostat. Small counts are always slightly modified so that no individual can be re-identified, while large counts and aggregated totals remain virtually exact. See [Statistical confidentiality](../references/statistical_confidentiality.md) for full details.

### Indicators

| KPI | Definition | Source table(s) | Tracking |
|-----|-----------|-----------------|----------|
| [Cumulative number of beneficiaries](cumulative_beneficiaries.md) | Total young people who have received a pass Culture credit | `exp_vidoc_beneficiary` | Quarterly |
| [Number of active beneficiaries](active_beneficiaries.md) | Beneficiaries with a non-expired, non-exhausted credit | `exp_vidoc_beneficiary` | Quarterly |
| [Beneficiary coverage rate](beneficiary_coverage_rate.md) | Share of eligible youth who received a credit | `exp_vidoc_beneficiary_coverage` + `exp_vidoc_population_coverage` | Annual |
| [Diversity rate (3+ categories)](diversity_3plus_categories.md) | Share who booked in 3+ categories at credit expiration | `exp_vidoc_diversity` | Quarterly |
| [Booking share by category](booking_share_by_category.md) | Share who booked in category X at credit expiration | `exp_vidoc_diversity_by_category` + `exp_vidoc_diversity` | Quarterly |
