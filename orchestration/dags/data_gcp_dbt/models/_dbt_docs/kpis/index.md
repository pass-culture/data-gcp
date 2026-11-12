---
title: KPIs
description: Definitions of the pass Culture key performance indicators, and the exports (VIDOC, data.gouv.fr) that publish them.
---

## KPIs

This section defines the pass Culture key performance indicators (KPIs), independently of the tool that displays them. For each indicator you will find the definition, the SQL calculation rule, the available dimensions, and where it is published.

The same indicator is published with the same figures in every tool: the exports only select and sum cells of a single perturbed base (see [A single perturbed base](../references/statistical_confidentiality.md#a-single-perturbed-base-for-every-export)).

| Tool | Audience | Tables |
|---|---|---|
| [VIDOC](../vidoc/index.md) | Ministry of Culture dashboard | `exp_vidoc__*` |
| [data.gouv.fr](../data_gouv/index.md) | Open data | `exp_data_gouv__*` |
| [Ministry exports](../ministry_kpis/index.md) | Ministry raw extracts | `exp_ministry__*` |

The data.gouv.fr resources are a subset of the VIDOC tables (beneficiaries, coverage and diversity).

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
