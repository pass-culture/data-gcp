# How to Test DAGs from Local Machine

## Overview

This guide explains how to test modified DAGs on any Airflow environment (dev, stg, prod) without affecting the live version.

**Key concept:** You create a **test copy** of your DAG with a different `dag_id` (e.g., `my_dag__test__john`), so both the original and test versions coexist without conflicts. The test copy has `schedule_interval=None` so it never runs automatically — only when you trigger it manually.

---

## Prerequisites

1. **GCloud CLI authenticated:**
   ```bash
   gcloud auth login
   gcloud config set project <your-gcp-project>
   ```

2. **Working from the `orchestration/` directory** — all `make` commands must be run from there:
   ```bash
   cd orchestration/
   ```

3. **Feature branch pushed to remote** — Airflow clones your branch to run the code:
   ```bash
   git push origin your-feature-branch
   ```

---

## Workflow

### One-step: create test copy and push

```bash
# Push to dev (default)
make test-dag DAG=my_dag.py

# Push to stg or prod
make test-dag DAG=my_dag.py ENV=stg
make test-dag DAG=my_dag.py ENV=prod
```

This finds the DAG file anywhere under `dags/` by filename, creates a local test copy with a modified `dag_id` and `schedule_interval=None`, then uploads it to GCS.

### Two-step: create locally, push separately

```bash
# Step 1 — create the test copy locally
make create-test-dag DAG=my_dag.py

# Step 2 — upload when ready
make push-dag DAG=my_dag__test__john.py ENV=stg
```

Use this when you want to review or edit the test copy before uploading.

---

## Step-by-Step Walkthrough

### Step 1: Prepare Your Feature Branch

```bash
git checkout -b feature/improve-analysis
git add jobs/etl_jobs/analytics/
git commit -m "feat: improve analysis logic"
git push origin feature/improve-analysis
```

**Record your branch name** — you'll need it when triggering the DAG.

---

### Step 2: Create and Push a Test DAG

```bash
cd orchestration/
make test-dag DAG=my_analysis_dag.py
```

**What happens:**
- Finds `my_analysis_dag.py` anywhere under `dags/`
- Creates `my_analysis_dag__test__<username>.py` next to it
- Changes `DAG_NAME`/`dag_id` to include the test suffix
- Sets `schedule_interval=None` (manual trigger only)
- Uploads the file to `gs://airflow-data-bucket-dev/dags/...`

**Expected output:**
```
✅ Test DAG created successfully!
Input : dags/jobs/analytics/my_analysis_dag.py
Output: dags/jobs/analytics/my_analysis_dag__test__john.py
DAG_NAME changed: "my_analysis_dag" → "my_analysis_dag__test__john"
schedule_interval changed: "get_airflow_schedule(schedule)" → "None"

🚀  dags/jobs/analytics/my_analysis_dag__test__john.py  →  gs://airflow-data-bucket-dev/dags/jobs/analytics/my_analysis_dag__test__john.py
✅ Done. DAG should appear in Airflow dev within 2-3 min.
```

---

### Step 3: Trigger the Test DAG

1. Open the Airflow UI for your environment (see links in [Resources](#resources))
2. Wait 2-3 minutes for Airflow to parse the new file
3. Search for your test DAG name (e.g., `my_analysis_dag__test__john`)
4. Click **▶️ Trigger DAG** and specify your branch in the config:
   ```json
   { "branch": "feature/improve-analysis" }
   ```

**Why specify the branch?** Most DAGs have a `branch` param that defaults to `master`. Setting it here makes Airflow clone your feature branch instead.

---

### Step 4: Monitor Execution

- Click into the DAG run to see task instances
- Check logs of `fetch_install_code` / `InstallDependenciesOperator` to confirm the right branch was cloned
- Verify results in BigQuery or wherever the DAG writes

---

## Cleanup

After testing and merging to master:

```bash
# Remove local test copy
rm dags/jobs/analytics/my_analysis_dag__test__john.py

# Remove from GCS (replace dev with the env you uploaded to)
gsutil rm gs://airflow-data-bucket-dev/dags/jobs/analytics/my_analysis_dag__test__john.py
```

> Test DAGs matching `*__test__*.py` are automatically purged from **all environments** after 14 days by the `purge_test_dags` DAG (runs twice daily at 3h and 18h).

---

## Makefile Reference

All commands run from `orchestration/`. `ENV` defaults to `dev`.

| Command | What it does |
|---|---|
| `make test-dag DAG=my_dag.py` | Create test copy + push to dev |
| `make test-dag DAG=my_dag.py ENV=stg` | Create test copy + push to stg |
| `make create-test-dag DAG=my_dag.py` | Create test copy locally only |
| `make push-dag DAG=my_dag.py [ENV=dev]` | Push an existing file to GCS |

`DAG=` accepts the filename only — no path needed. The Makefile finds the file under `dags/` automatically and errors if there are zero or multiple matches.

---

## `create_test_dag.py` Advanced Options

The script is called by `make create-test-dag` but can also be run directly for advanced use cases:

```bash
# Custom suffix instead of __test__<username>
./scripts/create_test_dag.py --suffix debug dags/path/to/my_dag.py

# Custom output path
./scripts/create_test_dag.py --output dags/test/my_test.py dags/path/to/my_dag.py

# Preview changes without creating the file
./scripts/create_test_dag.py --dry-run dags/path/to/my_dag.py
```

---

## Troubleshooting

**DAG doesn't appear in Airflow UI**
Wait up to 5 minutes. Check for parse errors in the Airflow UI. Verify the file exists in GCS: `gsutil ls gs://airflow-data-bucket-<env>/dags/...`

**Multiple DAGs found error**
Run `find dags -name "my_dag.py"` to see all matches, then delete or rename the duplicate you don't need.

**Wrong code version executing**
Verify the `branch` parameter in your trigger config. Check the `fetch_install_code` task logs.

**Permission denied on script**
```bash
chmod +x scripts/create_test_dag.py
```

---

## Resources {#resources}

| Environment | Airflow UI | GCS bucket |
|---|---|---|
| dev | https://airflow-dev.data.ehp.passculture.team/home | `gs://airflow-data-bucket-dev/dags/` |
| stg | https://airflow-stg.data.ehp.passculture.team/home | `gs://airflow-data-bucket-stg/dags/` |
| prod | https://airflow.data.passculture.team/home | `gs://airflow-data-bucket-prod/dags/` |
