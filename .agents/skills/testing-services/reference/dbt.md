# Testing dbt models

Covers `orchestration/dags/data_gcp_dbt/`. Run all `dbt` commands from that directory.

## Contents
- The four dbt test types and when to use each
- Schema-yml data tests (built-in, dbt_expectations, elementary)
- Custom generic tests (tests/generic/)
- Singular tests
- Unit tests (seeds + tag:unit_test)
- Severity configuration
- Running dbt tests

## The four test types — pick by intent

| Need | Type | Where it lives |
|---|---|---|
| Assert a column/table property (not_null, unique, accepted range, anomaly) | **data test** | `data_tests:` block in a model/source `.yml` |
| Reusable parameterized assertion across many models | **custom generic test** | `tests/generic/<name>.sql` |
| One-off assertion over a specific query result | **singular test** | `tests/<name>.sql` returning failing rows |
| Verify transformation logic against fixed mock input/output | **unit test** | seed + `data_tests` with `tag: unit_test` |

## Schema-yml data tests

Attach under `data_tests:` on a column, model, or source. Built-in (`not_null`, `unique`, `accepted_values`, `relationships`), plus `dbt_expectations.*` and `elementary.*` packages are available. Example from `models/sources.yml`:

```yaml
- name: applicative_database_booking
  data_tests:
    - elementary.volume_anomalies:
        config:
          tags: [elementary]
        arguments:
          timestamp_column: booking_creation_date
          anomaly_direction: both
          detection_period: { period: day, count: 1 }
          training_period: { period: month, count: 1 }
          anomaly_sensitivity: 3
```

When adding to a model, open its sibling `.yml` (or create one matching the existing pattern in that folder) and add column- or model-level `data_tests:`.

## Custom generic tests

Live in `tests/generic/*.sql` as `{% test name(model, column_name, ...) %}` macros that **select the failing rows / breach metric** (a non-empty result = failure). Existing examples to mirror: `not_null_proportion_test.sql`, `cardinality_equality.sql`, `kpi_variation_threshold.sql`, `generic_has_no_missing_partitions.sql`. They commonly read thresholds from `var(...)` (e.g. `not_null_anomaly_threshold_alert_percentage` in `dbt_project.yml`). Apply them like any data test:

```yaml
columns:
  - name: user_id
    data_tests:
      - not_null_proportion:
          arguments:
            where_condition: "true"
            anomaly_threshold_alert_percentage: 1
```

## Singular tests

A `.sql` file in `tests/` whose query returns rows only when the assertion fails. Use for a specific, non-reusable check. Keep them in BigQuery dialect (the project's warehouse).

## Unit tests (logic verification with mock data)

The repo verifies transformation logic with seeds enabled only in CI, tagged `unit_test`. Pattern (see `seeds/schema.yml`): a mock seed feeds a model, and `data_tests:` with named cases assert expected pass/fail via `severity`/`warn_if`/`error_if`:

```yaml
seeds:
  - name: mock_snapshot
    config:
      enabled: "{{ true if target.profile_name == 'CI' else false }}"
      tags: [unit_test]
    data_tests:
      - snapshot_volume_anomalies:
          arguments: { name: A1_massive_update_expected_FAIL, check_date: '2024-01-10', sensitivity: 3 }
          config: { severity: error, warn_if: "!=1", error_if: "!=1" }
```

Name each case with its expected outcome (`..._expected_FAIL` / `..._expected_PASS`) so the suite is self-documenting.

## Severity configuration

`dbt_project.yml` sets `data_tests` defaults: `severity` from `env_var('CI_SEVERITY', 'warn')`, tag `test`, and stricter `error` severity for `export` models on `prod`/`stg`. New tests inherit this — only override `severity` per test when the case requires it (as unit tests do).

## Running dbt tests

```bash
# from orchestration/dags/data_gcp_dbt
uv run dbt test -s "<model_name>"          # one model
uv run dbt test -s "tag:unit_test"         # the unit-test suite (CI target)
uv run dbt test -s "state:modified" --store-failures   # only changed nodes (CI on PRs)
```

If selection by `state:` is needed, dbt requires `--defer`/`--state` artifacts; prefer selecting by model name or tag locally. Confirm the test fails when expected (flip a value or inspect `--store-failures` output) before considering it done.

## Gap checklist

- [ ] Key columns have `not_null`/`unique`/`accepted_values` where the contract requires it
- [ ] Source freshness / volume anomalies on high-traffic sources
- [ ] Foreign-key style integrity via `relationships` or `cardinality_equality`
- [ ] Business KPIs guarded (e.g. `kpi_variation_threshold`) where regressions are costly
- [ ] Non-trivial transformation logic covered by a unit test with mock data
