---
title: VIDOC exports
description: Tables exported to VIDOC, the Ministry of Culture dashboard, and how they relate to the open data published on data.gouv.fr.
---

## VIDOC exports

VIDOC is the Ministry of Culture dashboard. pass Culture feeds it with the `exp_vidoc__*` tables (BigQuery dataset `export_vidoc_{env}`). The lot 1 tables are also sent to the ministry S3 bucket by the `export_vidoc` DAG, which only sends the columns validated by VIDOC (lot 1, then lot 2.1).

The KPIs computed from these tables are defined in [KPIs](../kpis/index.md).

### Tables

| Domain | Tables | Lot | Also published on data.gouv.fr |
|---|---|---|---|
| Beneficiaries | [beneficiary](../models/export/vidoc/description__exp_vidoc_beneficiary.md), [beneficiary_coverage](../models/export/vidoc/description__exp_vidoc_beneficiary__coverage.md), [population_coverage](../models/export/vidoc/description__exp_vidoc_population__coverage.md) | 1 | `beneficiary`, `beneficiary_coverage` (the INSEE population is not republished) |
| Diversity | [diversity](../models/export/vidoc/description__exp_vidoc_diversity.md), [diversity_by_category](../models/export/vidoc/description__exp_vidoc_diversity__by_category.md) | 1 (3+ categories), 2 (amounts, 1+ to 5+, spent share) | `diversity` (3+ categories only), `diversity_by_category` |
| Usage | [booking_beneficiary](../models/export/vidoc/description__exp_vidoc_booking_beneficiary.md) | 2 | – |
| Cultural partners | [cultural_partner](../models/export/vidoc/description__exp_vidoc_cultural_partner.md), [cultural_partner_by_type](../models/export/vidoc/description__exp_vidoc_cultural_partner__by_type.md), [booking_partner_finance_individual](../models/export/vidoc/description__exp_vidoc_booking_partner_finance__individual.md) | 2 | – |
| Collective (EAC) | [student_coverage](../models/export/vidoc/description__exp_vidoc_student__coverage.md), [institution_activity](../models/export/vidoc/description__exp_vidoc_institution__activity.md), [booking_collective](../models/export/vidoc/description__exp_vidoc_booking_collective.md), [booking_collective_venue](../models/export/vidoc/description__exp_vidoc_booking_collective_venue.md), [booking_collective_by_domain](../models/export/vidoc/description__exp_vidoc_booking_collective_by_domain.md), [booking_collective_by_format](../models/export/vidoc/description__exp_vidoc_booking_collective_by_format.md), [offer_collective_bookable](../models/export/vidoc/description__exp_vidoc_offer_collective_bookable.md) | 2 | – |
| Tops | [top_offer](../models/export/vidoc/description__exp_vidoc__top_offer.md), [top_offer_category](../models/export/vidoc/description__exp_vidoc__top_offer_category.md), [top_offer_labelled](../models/export/vidoc/description__exp_vidoc__top_offer_labelled.md), [top_venue](../models/export/vidoc/description__exp_vidoc__top_venue.md), [top_venue_labelled](../models/export/vidoc/description__exp_vidoc__top_venue_labelled.md) | – | – |

### Lot 2.1: diversity and credit use at expiration

The first lot 2 indicators are sent in the `diversity` table. VIDOC validated them in dev and staging, so prod sends them too. Dev and staging get all the columns of the exported tables, prod only the validated ones (`VIDOC_MODELS` in the `export_vidoc` DAG).

| Sheet | Indicator | Columns of `diversity` | Definition |
|---|---|---|---|
| FI2.3 | Average number of distinct venues booked at expiration | `total_distinct_venue_booked_at_expiration`, `total_expired_deposit_beneficiaries` | [Distinct venues booked](../kpis/diversity/distinct_venues_booked.md) |
| FI2.4 | Share of young people who booked in at least N categories (1 to 5) | `total_1plus_category_booked_beneficiaries` … `total_5plus_category_booked_beneficiaries`, `total_expired_deposit_beneficiaries` | [Diversity depth](../kpis/diversity/diversity_category_depth.md) |
| FI3.1 | Young people by share of credit spent | `total_spent_share_0_beneficiaries`, then `_0_10` … `_80_90`, `_90_100` (10-point steps) | [Spent share distribution](../kpis/usage/credit_spent_share_distribution.md) |
| FI3.2 | Average amount granted, spent, and spending rate | `total_deposit_amount_at_expiration`, `total_actual_amount_spent_at_expiration`, `total_expired_deposit_beneficiaries` | [Credit amounts at expiration](../kpis/usage/credit_amount_at_expiration.md) |

Counts of young people are perturbed (cell key method) and must be summed before dividing. Amounts and the number of venues are not perturbed: they are empty on rows with fewer than 5 young people before perturbation, so averages must leave those rows out of the numerator **and** the denominator.

### Open data

The lot 1 beneficiary and diversity tables are also published as open data on [data.gouv.fr](../data_gouv/index.md). Both exports read the same perturbed base (`exp_base__*`), so the open data figures are the VIDOC figures. The data.gouv.fr resources keep only the lot 1 columns and closed months, and format geographic codes for data.gouv.fr.

### Data protection

Beneficiary counts are perturbed with the cell key method; partner and finance figures follow the external reporting rules. See [Statistical confidentiality](../references/statistical_confidentiality.md).
