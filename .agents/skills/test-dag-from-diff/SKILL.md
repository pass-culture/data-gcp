---
name: test-dag-from-diff
description: "Detect which Airflow DAGs are affected by the current git changes, create test copies with a patched branch default pointing to the active branch, and upload them to GCS. Use this skill when the user wants to test DAG changes on a dev/stg environment, mentions 'test dag', 'push dag', 'deploy dag', or is on a feature branch and wants to validate orchestration changes."
---

# Test DAG from Diff Skill

Use this skill to automatically detect which Airflow DAGs are impacted by your current git changes, create test copies of those DAGs with the branch param defaulting to your active branch, and upload them to the target Airflow environment bucket.

## Instruction Flow

Follow these steps exactly when this skill is activated.

---

### 1. Gather Context

Run these two commands to establish the working context:

```bash
git branch --show-current
```

Store the result as `CURRENT_BRANCH`.

- If `CURRENT_BRANCH` is `master`, warn the user:
  > ⚠️ You are on `master`. The branch param in the test DAG will point to `master`, which is the same as the production default. Consider running this from a feature branch to test your specific changes.
  - Ask whether to proceed anyway.

Then ask the user for the target environment (default: `dev`):
> Which Airflow environment? [`dev` / `stg`] (default: `dev`)

Store the result as `ENV`.

---

### 2. Detect Changed Files

Run:

```bash
git diff --name-only $(git merge-base master HEAD)
```

If the branch has no commits yet (i.e., `merge-base` fails or returns no diff), fall back to:

```bash
git diff --name-only
```

Collect the list of changed file paths. If the list is empty, inform the user and stop:
> No changed files detected relative to master. Nothing to deploy.

---

### 3. Map Changed Files to DAGs

For each changed file path, determine a **search slug** using these rules:

| Changed file pattern | Slug |
|---|---|
| `jobs/ml_jobs/<folder>/...` | `<folder>` |
| `jobs/etl_jobs/external/<folder>/...` | `<folder>` |
| `jobs/etl_jobs/internal/<folder>/...` | `<folder>` |
| `jobs/etl_jobs/jobs/<folder>/...` | `<folder>` |
| `orchestration/dags/jobs/**/<file>.py` | The changed `.py` file itself is already a DAG — add it directly to the match list |
| Any other path | Skip silently |

For job slugs, search for matching DAGs in two ways:

**By filename** (most reliable):
```bash
find orchestration/dags -name "*<slug>*.py" -not -path "*/__pycache__/*" -not -name "__init__.py"
```

**By content** (as fallback if filename search returns nothing):
```bash
grep -rl "<slug>" orchestration/dags --include="*.py" | grep -v "__pycache__\|__init__.py"
```

Filter results to keep only files that contain a `DAG(` constructor (i.e., are actual DAG files, not config/helpers):
```bash
grep -l "DAG(" <candidate_files>
```

Deduplicate the final list across all changed files.

**If no DAGs are found**, inform the user and stop:
> No associated DAGs found in `orchestration/dags/` for the changed files. You may need to deploy manually.

---

### 4. Confirm with User

Present the matched DAGs clearly before doing anything:

```
The following DAGs were found for your changed files:

  1. orchestration/dags/jobs/ml/algo_training_graph_embeddings.py
     (matched by: jobs/ml_jobs/graph_recommendation/)

  2. orchestration/dags/jobs/ml/algo_training_two_towers.py
     (matched by: jobs/ml_jobs/algo_training/)

Proceed with creating and uploading test copies to ENV=dev? [y/n]
```

If the user declines or wants to adjust the list, let them specify which DAGs to include or exclude.

---

### 5. Create Test Copies

For each confirmed DAG (run from the `orchestration/` directory):

```bash
cd orchestration/
make create-test-dag DAG=<dag_stem>
```

Where `<dag_stem>` is the filename without `.py` (e.g. `algo_training_graph_embeddings`).

This produces a sibling file:
```
orchestration/dags/jobs/ml/algo_training_graph_embeddings__test__<username>.py
```

**If `make create-test-dag` fails**, report the error and stop — do not attempt to push a potentially broken file.

---

### 6. Patch Branch Default in Test Copy

**CRITICAL**: This step modifies only the test copy, never the original DAG file.

Get the username (same logic as `create_test_dag.py`):
```bash
USERNAME=$(whoami | tr '[:upper:]' '[:lower:]')
```

For each test copy, replace the hardcoded `"master"` branch default with `CURRENT_BRANCH`:

```bash
# macOS
sed -i '' 's/else "master"/else "'"${CURRENT_BRANCH}"'"/' "<test_copy_path>"

# Linux (if needed)
sed -i 's/else "master"/else "'"${CURRENT_BRANCH}"'"/' "<test_copy_path>"
```

The pattern `else "master"` targets the standard Airflow branch Param pattern used across this codebase:
```python
"branch": Param(
    default="production" if ENV_SHORT_NAME == "prod" else "master",  # ← patches this
    type="string",
),
```

After patching, verify the replacement worked:
```bash
grep "branch.*Param\|else \"${CURRENT_BRANCH}\"" "<test_copy_path>"
```

- If the grep for `else "master"` still appears → the patch failed. Report the issue and ask the user whether to push the unpatched copy or abort.
- If the DAG has no `branch` Param at all → skip this step silently (not all DAGs have it).

---

### 7. Upload to GCS

For each test copy (run from the `orchestration/` directory):

```bash
cd orchestration/
make push-dag DAG=<dag_stem>__test__<username> ENV=<env>
```

This uploads to:
```
gs://airflow-data-bucket-<env>/dags/jobs/.../<dag_stem>__test__<username>.py
```

**If `make push-dag` fails**, report the full error output. Do not continue with remaining DAGs until the user acknowledges.

---

### 8. Summary

Once all uploads are complete, present a concise summary:

```
✅ Test DAGs deployed to ENV=<env> (branch: <current_branch>)

  • <dag_stem>__test__<username>
    → gs://airflow-data-bucket-<env>/dags/jobs/<subfolder>/<dag_stem>__test__<username>.py

  [one line per deployed DAG]

DAGs should appear in Airflow <env> within 1-2 minutes.
Trigger them manually — schedule is set to None in test copies.
```

---

## Edge Cases

**Changed file maps to multiple DAGs**
Present all matches and let the user select which ones to deploy.

**DAG has no `branch` Param**
Skip the branch patch step for that DAG and note it in the summary.

**On master branch**
Warn but proceed if the user confirms. Branch default will remain `"master"` (sed will replace `"master"` with `"master"` — a no-op that's safe).

**Test copy already exists**
`make create-test-dag` uses `--force`, so it will overwrite. This is the expected behavior.

**`orchestration/` directory not found**
If the user is not in the repo root, resolve the path explicitly:
```bash
find . -name "Makefile" -path "*/orchestration/Makefile" | head -1 | xargs dirname
```
