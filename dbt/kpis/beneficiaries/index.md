## Beneficiaries

Young people who received a pass Culture credit, and the share of the eligible population they represent.

**Data protection**: Beneficiary counts are perturbed (cell key method). See [Statistical confidentiality](https://pass-culture.github.io/data-gcp/dbt/references/statistical_confidentiality/index.md).

| KPI                                                                                                                                          | Definition                                                               | VIDOC table(s)                                                     | data.gouv.fr           | Tracking                    |
| -------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ | ------------------------------------------------------------------ | ---------------------- | --------------------------- |
| [Number of ever-credited beneficiaries](https://pass-culture.github.io/data-gcp/dbt/kpis/beneficiaries/ever_credited_beneficiaries/index.md) | Young people who have ever received a pass Culture credit, active or not | `exp_vidoc_beneficiary`                                            | `beneficiary`          | Monthly                     |
| [Number of active beneficiaries](https://pass-culture.github.io/data-gcp/dbt/kpis/beneficiaries/active_beneficiaries/index.md)               | Beneficiaries with a non-expired, non-exhausted credit                   | `exp_vidoc_beneficiary`                                            | `beneficiary`          | Monthly                     |
| [Beneficiary coverage rate](https://pass-culture.github.io/data-gcp/dbt/kpis/beneficiaries/beneficiary_coverage_rate/index.md)               | Share of eligible youth who received a credit                            | `exp_vidoc_beneficiary_coverage` + `exp_vidoc_population_coverage` | `beneficiary_coverage` | Monthly (rolling 12 months) |
