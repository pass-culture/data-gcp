---
title: "KPIs: Beneficiaries"
description: "Young people who received a pass Culture credit, and the share of the eligible population they represent."
---

## Beneficiaries

Young people who received a pass Culture credit, and the share of the eligible population they represent.

**Data protection**: Beneficiary counts are perturbed (cell key method). See [Statistical confidentiality](../../references/statistical_confidentiality.md).

| KPI | Definition | VIDOC table(s) | data.gouv.fr | Tracking |
|-----|-----------|-----------------|--------------|----------|
| [Number of ever-credited beneficiaries](ever_credited_beneficiaries.md) | Young people who have ever received a pass Culture credit, active or not | `exp_vidoc_beneficiary` | `beneficiary` | Monthly |
| [Number of active beneficiaries](active_beneficiaries.md) | Beneficiaries with a non-expired, non-exhausted credit | `exp_vidoc_beneficiary` | `beneficiary` | Monthly |
| [Beneficiary coverage rate](beneficiary_coverage_rate.md) | Share of eligible youth who received a credit | `exp_vidoc_beneficiary_coverage` + `exp_vidoc_population_coverage` | `beneficiary_coverage` | Monthly (rolling 12 months) |
