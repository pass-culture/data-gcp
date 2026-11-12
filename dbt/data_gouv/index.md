## data.gouv.fr exports

pass Culture publishes open data on [data.gouv.fr](https://www.data.gouv.fr), tagged `culture`: indicators on the young beneficiaries of the individual part of the pass Culture (outside school time), by month and territory of residence. The collective part (school time) is not covered.

Only pass Culture data is published. The INSEE population estimates used as the coverage rate denominator are not republished; their computation is public in [metrics_population\_\_coverage](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models/metrics/population).

### Resources

| Resource (CSV)          | Content                                                                                      | dbt model                                                                                                                                                                | KPIs                                                                                                                                                                                                                                          |
| ----------------------- | -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `beneficiary`           | Monthly stock of beneficiaries and active beneficiaries, by department, age, QPV and density | [exp_data_gouv\_\_beneficiary](https://pass-culture.github.io/data-gcp/dbt/models/export/data_gouv/description__exp_data_gouv__beneficiary/index.md)                     | [Ever-credited](https://pass-culture.github.io/data-gcp/dbt/kpis/beneficiaries/ever_credited_beneficiaries/index.md) and [active](https://pass-culture.github.io/data-gcp/dbt/kpis/beneficiaries/active_beneficiaries/index.md) beneficiaries |
| `beneficiary_coverage`  | Beneficiaries who reached an age (16 to 19) over the last 12 months, by department           | [exp_data_gouv\_\_beneficiary_coverage](https://pass-culture.github.io/data-gcp/dbt/models/export/data_gouv/description__exp_data_gouv__beneficiary_coverage/index.md)   | [Coverage rate](https://pass-culture.github.io/data-gcp/dbt/kpis/beneficiaries/beneficiary_coverage_rate/index.md) numerator                                                                                                                  |
| `diversity`             | Beneficiaries whose credit expired in the month, and those who booked in 3+ categories       | [exp_data_gouv\_\_diversity](https://pass-culture.github.io/data-gcp/dbt/models/export/data_gouv/description__exp_data_gouv__diversity/index.md)                         | [Diversity rate (3+ categories)](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/diversity_3plus_categories/index.md)                                                                                                              |
| `diversity_by_category` | Same population, by offer category booked                                                    | [exp_data_gouv\_\_diversity_by_category](https://pass-culture.github.io/data-gcp/dbt/models/export/data_gouv/description__exp_data_gouv__diversity_by_category/index.md) | [Booking share by category](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/booking_share_by_category/index.md)                                                                                                                    |

Every model reads the perturbed base `exp_base__*` (see [A single perturbed base](https://pass-culture.github.io/data-gcp/dbt/references/statistical_confidentiality/#a-single-perturbed-base-for-every-export)), so the published figures are the ones pass Culture uses in all its reporting.

### Format

- **Months**: closed months only; the current month is excluded.
- **Geography**: INSEE COG codes as strings, so that data.gouv.fr detects them (`region_code` zero-padded, e.g. `01` for Guadeloupe). Codes are empty for non-localised beneficiaries (label `non localisé` kept so that totals stay complete) and `region_code` is empty for overseas collectivities (975, 978, 986, 987, 988).
- **CSV**: UTF-8, comma separator, ISO dates (first day of the month), lowercase booleans; an empty cell means "unknown", never zero.

### Data protection

Counts of people are protected with the [Cell Key Perturbation method](https://pass-culture.github.io/data-gcp/dbt/references/statistical_confidentiality/index.md): a published count is 0 or >= 5, and nested counts are capped at their parent count. dbt tests on each model check the grain, the 0 or >= 5 rule, the nested caps and the code formats.

### Schemas

Each resource has a [Table Schema](https://specs.frictionlessdata.io/table-schema/) in French (field titles, definitions, constraints), generated by [`data_gouv/build_schemas.py`](https://github.com/pass-culture/data-gcp/tree/master/data_gouv) and versioned under `data_gouv/schemas/<resource>/<version>/schema.json`. The `exp_data_gouv__*` models have an enforced dbt contract, so a column change fails the build: bump the schema version, regenerate, and update the resource `schema` on data.gouv.fr.

### Publication

See the [data_gouv README](https://github.com/pass-culture/data-gcp/tree/master/data_gouv) for the export and upload process.
