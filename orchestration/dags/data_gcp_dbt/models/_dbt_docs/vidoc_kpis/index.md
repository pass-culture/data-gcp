---
title: Vidoc KPIs
description: Key performance indicators exported for the ministry's vidoc visualisation tool.
---

## Vidoc KPIs

This section documents the key performance indicators (KPIs) exported for the ministry via the **vidoc** export tables. For each indicator you will find the definition, source table(s), SQL calculation rule, and available dimensions.

All vidoc export tables are documented individually in the **Table definitions** section under `export > vidoc`.

### Data protection

All exported counts are protected using the **Cell Key Perturbation method** — the current standard recommended by INSEE and Eurostat. Small counts are always slightly modified so that no individual can be re-identified, while large counts and aggregated totals remain virtually exact. See [Statistical confidentiality](../references/statistical_confidentiality.md) for full details.

The EAC (collective part) tables are the exception: `exp_vidoc_student_coverage` holds ADAGE aggregates of school headcounts and `exp_vidoc_institution_activity` counts institutions (public bodies) and their credit, so neither is perturbed.
The booking exports follow the external reporting rules instead. Cultural partner revenue (`exp_vidoc_booking_partner_finance_individual`) is protected by **statistical secret**: departments secret at the last complete month (fewer than 3 partners with revenue, one partner above 85% of the revenue, or secondary secret) are hidden on every month. Bookings and spending of young people by territory of residence (`exp_vidoc_booking_beneficiary`) are hidden on every month when fewer than 5 beneficiaries account for them (20 for the category breakdown).

### Indicators

| KPI | Definition | Source table(s) | Tracking |
|-----|-----------|-----------------|----------|
| [Number of ever-credited beneficiaries](ever_credited_beneficiaries.md) | Young people who have ever received a pass Culture credit, active or not | `exp_vidoc_beneficiary` | Monthly |
| [Number of active beneficiaries](active_beneficiaries.md) | Beneficiaries with a non-expired, non-exhausted credit | `exp_vidoc_beneficiary` | Monthly |
| [Beneficiary coverage rate](beneficiary_coverage_rate.md) | Share of eligible youth who received a credit | `exp_vidoc_beneficiary_coverage` + `exp_vidoc_population_coverage` | Monthly (rolling 12 months) |
| [Diversity rate (3+ categories)](diversity_3plus_categories.md) | Share who booked in 3+ categories at credit expiration | `exp_vidoc_diversity` | Monthly |
| [Booking share by category](booking_share_by_category.md) | Share who booked in category X at credit expiration | `exp_vidoc_diversity_by_category` + `exp_vidoc_diversity` | Monthly |
| [Student EAC participation rate](student_eac_participation_rate.md) | Share of eligible students engaged in at least one EAC action | `exp_vidoc_student_coverage` | School year (monthly, cumulative) |
| [Institution EAC participation rate](institution_eac_participation_rate.md) | Share of credited institutions with at least one collective booking | `exp_vidoc_institution_activity` | School year (monthly, cumulative) |
| [EAC credit consumption rate](eac_credit_consumption_rate.md) | Share of the collective credit allocated to institutions that was engaged | `exp_vidoc_institution_activity` | School year (monthly, cumulative) |
| [EAC collective bookings](eac_collective_bookings.md) | Confirmed collective bookings and amount, by school (default) or cultural partner geography | `exp_vidoc_booking_collective` + `exp_vidoc_booking_collective_venue` | Monthly (scholar year) |
| [EAC bookings by educational domain](eac_bookings_by_domain.md) | Confirmed collective bookings, amount, tickets and schools by domain (domains not summable) | `exp_vidoc_booking_collective_by_domain` | Monthly (scholar year) |
| [EAC bookings by offer format](eac_bookings_by_format.md) | Confirmed collective bookings, amount, tickets and schools by format (formats not summable) | `exp_vidoc_booking_collective_by_format` | Monthly (scholar year) |
| [EAC bookable collective offers](eac_bookable_offers.md) | Bookable (non showcase) collective offers created and pre-reserved, and partners creating them | `exp_vidoc_offer_collective_bookable` | Monthly (scholar year) |
| [Number of active cultural partners by channel](active_cultural_partners.md) | Venues with a bookable offer in the last 365 days: individual only, collective only, both | `exp_vidoc_cultural_partner` | Monthly (rolling 365 days) |
| [Cumulative number of activated cultural partners](activated_cultural_partners.md) | Venues that have had at least one bookable offer since 2022-06-10 | `exp_vidoc_cultural_partner` | Monthly (cumulative) |
| [Number of active and activated cultural partners by type](active_cultural_partners_by_type.md) | Active and activated venues for one partner type (never summed over types) | `exp_vidoc_cultural_partner_by_type` | Monthly (rolling 365 days) |
| [Diversity depth (1+ to 5+ categories)](diversity_category_depth.md) | Share who booked in at least N categories at credit expiration, N = 1 to 5 | `exp_vidoc_diversity` | Monthly |
| [Amount granted, spent and spending rate](credit_amount_at_expiration.md) | Average amount granted and spent per beneficiary, and share of credit spent, at expiration | `exp_vidoc_diversity` | Monthly |
| [Distribution by share of credit spent](credit_spent_share_distribution.md) | Beneficiaries by bucket of share of credit spent at expiration | `exp_vidoc_diversity` | Monthly |
| [Distinct venues booked](distinct_venues_booked.md) | Average number of distinct venues booked per beneficiary at expiration | `exp_vidoc_diversity` | Monthly |
| [Individual bookings and tickets by venue territory](individual_bookings_venue_territory.md) | Used individual bookings and tickets in a payment batch, by venue department | `exp_vidoc_booking_partner_finance_individual` | Monthly |
| [Individual booking revenue by venue territory](individual_revenue_venue_territory.md) | Partner revenue, reimbursed and contributed amounts, by venue department | `exp_vidoc_booking_partner_finance_individual` | Monthly |
| [Individual bookings by category and beneficiary territory](individual_bookings_beneficiary_territory.md) | Used individual bookings and amount spent by young people, by department of residence and category | `exp_vidoc_booking_beneficiary` | Monthly |
