---
name: migrate-dag-to-new-dc
description: "Migrate an Airflow DAG that already uses CustomKubernetesPodOperator to the new (French) datacenter. Conforms the DAG to the new-infra KPO pattern (service_account_name, runtime_mode, params.branch) and registers it in orchestration/scripts/migrated_dags.yaml, filling tables_to_copy only when the DAG writes incrementally (append / merge / partitioned load). Use this skill when the user wants to migrate a DAG to the new DC, mentions migrated_dags, 'migrate dag', 'nouveau datacenter', or is moving ETL/import DAGs onto the new infra."
argument-hint: "Path to the DAG file to migrate, or the DAG name"
---

# Migrate DAG to New Datacenter

Automates the migration of an Airflow DAG (already using `CustomKubernetesPodOperator`) onto the new French datacenter.

Two things must be done for a complete migration:

1. **Conform the DAG file** to the new-infra KPO pattern.
2. **Register the DAG** in `orchestration/scripts/migrated_dags.yaml`, filling `tables_to_copy`
   **only** when the DAG writes its data **incrementally** (so historical data must be copied
   from the old DC). Full-refresh DAGs get an empty `tables_to_copy`.

> ⚠️ This skill assumes the DAG **already uses** `CustomKubernetesPodOperator`. If the DAG does not
> use it, stop: this skill cannot be applied (we are not migrating those DAGs onto the new infra for now).

---

## Instruction Flow

### 0. Check if this is a standard ETL job

Before applying this skill, determine whether the DAG is a standard ETL/import job that should use the
default ETL service account.

- If the DAG is a standard ETL job (e.g. `jobs/etl_jobs/...`, `external/*`, `internal/*`, import jobs),
  continue normally and use:
  `f"sa-airflow-dags-rcs-{ENV_SHORT_NAME}-etl"`
- Otherwise, stop and ask the user for the exact service account to use for this DAG.
  Do not try to guess or hardcode a different value.

This skill is only for standard ETL jobs. Non-ETL jobs may require a different service account,
custom IAM, or project-specific permissions.

---

### 1. Locate and Read the DAG

Resolve the DAG file (argument may be a path or a DAG name):

```bash
find orchestration/dags -name "*<name>*.py" -not -path "*/__pycache__/*" -not -name "__init__.py"
```

Read the DAG and extract:
- `DAG_NAME`
- `MICROSERVICE_PATH` (e.g. `jobs/etl_jobs/external/adage`)
- Every `CustomKubernetesPodOperator` task and its `arguments`

Confirm the DAG uses `CustomKubernetesPodOperator`. If not, stop and tell the user the DAG does not
use `CustomKubernetesPodOperator`, so this skill cannot be applied.

---

### 2. Conform the DAG to the New-Infra KPO Pattern

Check (and fix if missing) that **every** `CustomKubernetesPodOperator` task has:

| Field | Expected value |
|-------|----------------|
| `orchestration_mode` | `"celery"` |
| `queue` | `"k8s-watcher"` |
| `runtime_mode` | `"gitsynced"` (ETL default) |
| `runtime_branch` | `"{{ params.branch }}"` |
| `runtime_image` | `"py313"` (or the image the microservice needs) |
| `runtime_image_tag` | `"v1"` |
| `microservice_path` | the job path |
| `service_account_name` | `f"sa-airflow-dags-rcs-{ENV_SHORT_NAME}-etl"` |
| `container_resources` | `DEFAULT_CONTAINER_RESOURCES` (or custom) |

Check the DAG-level config:
- `params.branch` Param present, defaulting to `"production"` on prod (`ENV_SHORT_NAME in ("prod","prd")`)
  and `"master"` otherwise.
- `service_account_name` on each `CustomKubernetesPodOperator` task:
  `f"sa-airflow-dags-rcs-{ENV_SHORT_NAME}-etl"`
- `tags` include `DAG_TAGS.POD.value`.
- Required imports present: `CustomKubernetesPodOperator`, `DEFAULT_CONTAINER_RESOURCES`,
  `ENV_SHORT_NAME`, `GCP_PROJECT_ID`.

Use the already-migrated DAGs as reference (same folder):
`import_gsheet.py`.

Apply the minimal edits needed. If the DAG already matches the pattern, say so and move on —
don't rewrite working code.

---

### 3. Determine Incremental vs Full-Refresh (decides `tables_to_copy`)

This is the core decision. Read the microservice at `MICROSERVICE_PATH` (`main.py` and the
`scripts/` it calls) and classify **each destination table**.

**Incremental → MUST be copied** (data accumulates; the DAG only appends new data):
- `pandas_gbq.to_gbq(..., if_exists="append")`
- `bigquery.WriteDisposition.WRITE_APPEND`
- `MERGE ... INTO` a historical/accumulating table
- A date-partitioned load that writes only the run's partition (daily/backfill pattern)
- Any "since_date" / incremental window logic

**Full-refresh → NOT copied** (leave `tables_to_copy: []`; the DAG rebuilds everything on next run):
- `pandas_gbq.to_gbq(..., if_exists="replace")`
- `bigquery.WriteDisposition.WRITE_TRUNCATE` over the whole table
- A full dump of the external source each run (e.g. gsheet)

Also identify the **`source_dataset`** (the base, un-suffixed dataset the DAG writes to — usually
`raw`; the copy tool appends the env suffix automatically, e.g. `raw_prod` → `raw_prd`).

> Note: `migrated_dags.yaml` supports **one `source_dataset` per DAG**. If a DAG writes
> incrementally to several datasets, list only the dataset holding the irreplaceable raw history,
> and flag the extra datasets to the user. Analytics/intermediate tables rebuilt by dbt or
> re-derivable from raw do **not** need copying.

Present a short classification table to the user before writing anything:

```
DAG: import_adage_v1   microservice: jobs/etl_jobs/external/adage
source_dataset: raw

  raw.adage                   append  → INCREMENTAL → copy
  raw.adage_involved_student  append  → INCREMENTAL → copy

Proposed tables_to_copy: [adage, adage_involved_student]
```

For a full-refresh DAG, show that `tables_to_copy` will be `[]` with the reason.

---

### 4. Register the DAG in `migrated_dags.yaml`

Edit `orchestration/scripts/migrated_dags.yaml`. Add an entry under `dags_in_paris` keyed by
`DAG_NAME`:

```yaml
dags_in_paris:
  # ...existing entries...
  import_adage_v1:
    source_dataset: raw
    tables_to_copy: [adage, adage_involved_student]  # incremental tables → copy history
```

For a full-refresh DAG:

```yaml
  import_gsheet:
    source_dataset: raw
    tables_to_copy: [] # full refresh -> no tables to copy
```

Rules:
- Key = exact `DAG_NAME` from the DAG file (the copy tool looks it up by this name).
- Do not touch the `environments:` block.
- Keep entries sorted/grouped consistently with what's already there.

---

### 5. Validate

- `get_errors` on the edited DAG file — fix syntax/import issues.
- YAML sanity check:

```bash
cd orchestration && python -c "import yaml,sys; yaml.safe_load(open('scripts/migrated_dags.yaml')); print('yaml ok')"
```

- Confirm the microservice is pushed on the branch referenced by `params.branch`
  (`runtime_branch` fetches code at runtime).

---

### 6. Output the Copy Commands (DO NOT run them)

**Never run the copy yourself.** The copy tool reads `migrated_dags.yaml`, shows a per-table cost
estimate, asks for confirmation, copies (`WRITE_TRUNCATE`) from the old-DC project to the new-DC
project, and verifies row counts / schema / partition bounds — but the **user runs it later**.

If `tables_to_copy` is **empty**, output a single line stating no copy is needed (the DAG rebuilds
its data on its next run) and skip the rest of this step.

If `tables_to_copy` is **non-empty**, print a ready-to-run block with the command for **all three
environments**, to be run from the `orchestration/` directory (prod last, after validating
`tst`/`stg`):

```
📋 Data copy required for <DAG_NAME> (tables: <tables_to_copy>).
Run these later from the `orchestration/` directory — validate tst/stg before prd:

cd orchestration

# 1. test
python -m scripts.bigquery_copy.main --dag-name <DAG_NAME> --env tst

# 2. staging
python -m scripts.bigquery_copy.main --dag-name <DAG_NAME> --env stg

# 3. production (run last, only after tst/stg are OK)
python -m scripts.bigquery_copy.main --dag-name <DAG_NAME> --env prd

# Inspect recent copy jobs for an env:
# python -m scripts.bigquery_copy.main --env <tst|stg|prd> --list-copy-jobs
```

The tool prints a plan and prompts for confirmation before copying.

---

### 7. Summary

Report:
- DAG pattern changes applied (or "already conform").
- Classification (incremental vs full-refresh) and the resulting `tables_to_copy`.
- `migrated_dags.yaml` entry added.
- The copy commands for the 3 environments (from step 6) **for the user to run later** — or a note
  that no copy is needed for full-refresh DAGs. Never run the copy yourself.
- Reminder to push the microservice on the `runtime_branch` and to test the DAG
  (`make test-dag DAG=<name> ENV=dev`) before prod.

---

## Reference: Already-Migrated Examples

| DAG | Write pattern | `tables_to_copy` |
|-----|---------------|------------------|
| `import_gsheet` | full dump each run (`replace`) | `[]` |
| `import_adage_v1` | `append` + `MERGE` into historical | `[adage, adage_involved_student]` |

## Common Pitfalls

| Pitfall | Fix |
|---------|-----|
| Copying full-refresh tables | Wasteful + overwritten on next run — leave `tables_to_copy: []` |
| Forgetting incremental history | Appended/partitioned raw tables are irreplaceable — must be copied |
| Wrong `source_dataset` | Use the base name (`raw`), not the suffixed one (`raw_prod`) |
| Key mismatch | `dags_in_paris` key must equal the exact `DAG_NAME` |
| Copying dbt-rebuilt tables | Analytics/intermediate tables rebuilt by dbt don't need copying |
| Running copy before DAG is live on new DC | Copy history first, then let the DAG resume appends |
| Running the copy yourself | Never run the copy — only output the 3 commands for the user to run later |
| Running the copy tool from the wrong dir | Run `python -m scripts.bigquery_copy.main` from `orchestration/` |
