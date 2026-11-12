---
title: Vidoc KPIs
description: Key performance indicators exported for the ministry's vidoc visualisation tool.
---

## Vidoc KPIs

This section documents the key performance indicators (KPIs) exported for the ministry via the **vidoc** export tables. For each indicator you will find the definition, source table(s), SQL calculation rule, and available dimensions.

All vidoc export tables are documented individually in the **Table definitions** section under `export > vidoc`.

### Data protection

Counts of people are protected using the **Cell Key Perturbation method**, the standard recommended by INSEE and Eurostat: small counts are slightly modified so that no individual can be re-identified, while large counts and totals remain virtually exact. Amounts and bookings follow the external reporting rules (hidden small counts, statistical secret on partner revenue). Each domain page states its rules; see [Statistical confidentiality](../references/statistical_confidentiality.md) for full details.

### Domains

| Domain | Indicators |
|--------|-----------|
| [Beneficiaries](beneficiaries/index.md) | Young people who received a pass Culture credit, and the share of the eligible population they represent. |
| [Diversity](diversity/index.md) | Diversity of the cultural practices of young people at credit expiration: number of categories booked, categories and distinct venues. |
| [Usage](usage/index.md) | Use of the individual credit by young people: amounts granted and spent at expiration, and bookings by territory of residence and category. |
| [Cultural partners](partners/index.md) | Active and activated cultural partners, and their bookings and revenue from individual bookings by venue territory. |
| [Collective (EAC)](collective/index.md) | Collective part of the pass Culture (EAC): school and student participation, credit consumption, collective bookings and offers. |
| [Tops](tops/index.md) | Monthly top 50 offers and cultural venues by used booking revenue, by geographic level, category and ministry label. |
