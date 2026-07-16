KPI table computing the rolling 12-month French population coverage from the [`int_seed__monthly_insee_population_estimation`](#!/model/model.data_gcp_dbt.int_seed__monthly_insee_population_estimation) seed (the `data-insee-population` job estimation).

It restricts the population to the milestone ages 15-20 and adds, for each `milestone_age` / `department_code`, a 12-month rolling sum of the estimated population.

This model is the counterpart of the legacy [`int_kpi__population_coverage`](#!/model/model.data_gcp_dbt.int_kpi__population_coverage) and is kept in parallel to compare both estimations before migration.
