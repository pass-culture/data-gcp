# How to Test DAGs from Local Machine to Staging

## Overview

This guide explains how to test modified DAGs on Airflow Staging **before merging to production**. The process allows you to:

- Deploy modified DAGs to Staging without affecting the production version
- Test business logic changes on your feature branch
- Iterate rapidly without impacting other developers
- Verify everything works before merging to master

**Key concept:** You create a **test copy** of your DAG with a different name (e.g., `my_dag_test_john`), so both the original and test versions coexist on Staging without conflicts.

---

## Prerequisites

Before you begin, ensure you have:

1. **GCloud CLI authenticated:**
   ```bash
   gcloud auth login
   gcloud config set project <your-gcp-project>
   ```

2. **gsutil access to staging bucket:**
   ```bash
   # Test access
   gcloud storage ls gs://airflow-data-bucket-stg/dags/
   ```

3. **Git setup:**
   - Working in a feature branch with your changes
   - Ability to push to remote repository

4. **Scripts location:**
   - `orchestration/scripts/push_to_staging.sh` - Upload DAG files
   - `orchestration/scripts/create_test_dag.py` - Create test copies

---

## Understanding Branch Isolation

**Important:** Test DAGs and production DAGs are completely isolated through branch configuration.

### How it Works:

1. **Original DAG** (e.g., `my_dag`):
   - Exists on Staging with default branch parameter
   - On Staging, defaults to `master` branch
   - Continues working normally, unaffected by your changes

2. **Test DAG** (e.g., `my_dag_test_john`):
   - New DAG file with modified `dag_id`
   - You explicitly specify your feature branch when triggering
   - Pulls code from YOUR branch, not master

3. **No Conflicts:**
   - Different DAG names → Airflow treats them as separate DAGs
   - Different branch parameters → They access different code versions
   - Both can run simultaneously

### Why This is Safe:

- Pushing changes to your feature branch does NOT affect the original DAG
- The original DAG still uses `master` branch by default
- Your test DAG only uses your branch when you explicitly specify it
- After testing, you merge to master and clean up the test DAG

---

## Step-by-Step Workflow

### Step 1: Prepare Your Feature Branch

Make sure your business logic changes are committed and pushed:

```bash
# Check current branch
git branch

# Add your changes
git add jobs/etl_jobs/...

# Commit
git commit -m "Feature: Description of changes"

# Push to remote (Airflow needs remote access)
git push origin your-feature-branch
```

**Record your branch name** - you'll need it later when triggering the DAG.

---

### Step 2: Create a Test Copy of Your DAG

Use the `create_test_dag.py` script to create a test version:

```bash
# Basic usage (adds _test_<username> suffix automatically)
./orchestration/scripts/create_test_dag.py orchestration/dags/path/to/my_dag.py
```

**What happens:**
- Creates NEW file: `my_dag_test_<username>.py`
- Modifies `dag_id` inside the file to include test suffix
- Original file remains completely untouched

**Advanced options:**

```bash
# Use custom suffix
./orchestration/scripts/create_test_dag.py --suffix v2 orchestration/dags/path/to/my_dag.py
# Creates: my_dag_v2.py

# Specify custom output path
./orchestration/scripts/create_test_dag.py --output orchestration/dags/test/my_test.py orchestration/dags/path/to/my_dag.py

# Dry-run to preview changes
./orchestration/scripts/create_test_dag.py --dry-run orchestration/dags/path/to/my_dag.py
```

**Expected output:**
```
✅ Test DAG created successfully!
Input:  orchestration/dags/path/to/my_dag.py
Output: orchestration/dags/path/to/my_dag_test_john.py
DAG_NAME changed: "my_dag" → "my_dag_test_john"
```

**Verify:**
```bash
# Check the test file was created
ls -la orchestration/dags/path/to/my_dag_test_*.py

# Verify DAG_NAME was changed
grep "DAG_NAME\|dag_id" orchestration/dags/path/to/my_dag_test_*.py

# Verify original is untouched
git diff orchestration/dags/path/to/my_dag.py
# Should show: no changes
```

---

### Step 3: Upload Test DAG to GCS Staging

Use the `push_to_staging.sh` script to upload:

```bash
# Upload your test DAG
./orchestration/scripts/push_to_staging.sh orchestration/dags/path/to/my_dag_test_john.py
```

**What happens:**
- Validates file exists and is in correct location
- Transforms path to GCS destination
- Uploads using `gsutil cp`

**Advanced options:**

```bash
# Dry-run to preview upload
./orchestration/scripts/push_to_staging.sh --dry-run orchestration/dags/path/to/my_dag_test_john.py

# Works with absolute paths too
./orchestration/scripts/push_to_staging.sh /full/path/to/data-gcp/orchestration/dags/path/to/my_dag_test_john.py
```

**Expected output:**
```
🚀 Uploading file to staging environment...
From (Local) : orchestration/dags/path/to/my_dag_test_john.py
To   (GCS)   : gs://airflow-data-bucket-stg/dags/jobs/admin/my_dag_test_john.py
------------------------------------------------------------
Copying file://...
Operation completed over 1 objects/X.X KiB.
```

**Verify on GCS:**
```bash
# Check test DAG exists
gsutil ls gs://airflow-data-bucket-stg/dags/jobs/admin/ | grep test

# Verify original still exists
gsutil ls gs://airflow-data-bucket-stg/dags/jobs/admin/my_dag.py

# View uploaded content
gsutil cat gs://airflow-data-bucket-stg/dags/jobs/admin/my_dag_test_john.py | head -30
```

---

### Step 4: Trigger DAG on Airflow Staging

#### Access Airflow UI:

1. Open Airflow Staging: `https://airflow-stg.data.ehp.passculture.team/home`
2. **Wait 2-3 minutes** for Airflow to detect the new DAG (parser runs on interval)

#### Find Your Test DAG:

1. Use the search box at the top
2. Search for your test DAG name (e.g., `my_dag_test_john`)
3. Both DAGs should appear:
   - `my_dag` (original)
   - `my_dag_test_john` (your test version)

#### Verify Code Upload:

1. Click on your test DAG name
2. Go to **Code** tab
3. Verify the DAG_NAME or dag_id includes your test suffix

#### Trigger with Your Feature Branch:

1. Click the **▶️ Play button** (top right)
2. In the **"Trigger DAG"** modal, expand **"Configuration (JSON)"**
3. Specify your branch:
   ```json
   {
     "branch": "your-feature-branch",
     "other_param": "value"
   }
   ```
4. Click **"Trigger"**

**Key point:** The `branch` parameter tells Airflow which Git branch to clone for accessing your business logic code.

#### Default Branch Behavior:

Most DAGs have a branch parameter like this:
```python
"branch": Param(
    default="production" if ENV_SHORT_NAME == "prod" else "master",
    type="string"
)
```

- **On Staging:** Defaults to `master` if not specified
- **Your test DAG:** Specify your feature branch to use your changes
- **Original DAG:** Still uses `master` by default (unaffected)

---

### Step 5: Monitor Execution

#### In Airflow UI:

1. Click on your test DAG run to see task instances
2. Common tasks to check:
   - `gce_start_task` - GCE instance startup
   - `fetch_install_code` / `InstallDependenciesOperator` - Clone branch and install dependencies
   - Main business logic tasks
   - `gce_stop_task` - Cleanup

#### Check Logs:

1. Click on individual tasks to view logs
2. **Verify correct branch:** Check logs of `fetch_install_code` task for branch name
3. Look for errors or unexpected behavior

#### Verify Results:

- Check BigQuery tables or wherever your DAG writes data
- Compare with expected outcomes

---

## Troubleshooting

### DAG Doesn't Appear in Airflow UI

**Cause:** Airflow hasn't parsed the new DAG file yet

**Solution:**
- Wait up to 5 minutes
- Check for "Parse Errors" in Airflow UI
- Verify upload with: `gsutil ls gs://airflow-data-bucket-stg/dags/...`

### Wrong Code Version Executing

**Cause:** Airflow using cached version or wrong branch

**Solution:**
- Verify branch parameter in trigger config
- Wait for Airflow to re-parse (2-3 minutes)
- Check Code tab in UI shows latest version

### Business Logic Not Found

**Cause:** Git branch not accessible to Airflow

**Solution:**
- Verify branch is pushed to remote: `git push origin your-branch`
- Check branch name spelling in trigger config
- Verify InstallDependenciesOperator logs for clone errors

### Import Errors

**Cause:** Missing dependencies or Python path issues

**Solution:**
- Check `pyproject.toml` has all required dependencies
- Verify dependencies are available on Staging environment
- Check task logs for specific import error details

### Script Permission Denied

**Cause:** Scripts not executable

**Solution:**
```bash
chmod +x orchestration/scripts/push_to_staging.sh
chmod +x orchestration/scripts/create_test_dag.py
```

### Upload Path Validation Error

**Cause:** File not in `orchestration/dags/` directory

**Solution:**
- Only files within `orchestration/dags/` can be uploaded
- Check your file path is correct
- Use relative or absolute paths pointing to valid DAG files

---

## Cleanup

After testing is complete and changes are merged to master:

### 1. Delete Test DAG File Locally:

```bash
# Remove the test copy
rm orchestration/dags/path/to/my_dag_test_john.py
```

### 2. Remove Test DAG from GCS:

```bash
# Delete from staging bucket
gsutil rm gs://airflow-data-bucket-stg/dags/jobs/admin/my_dag_test_john.py
```

### 3. Delete Feature Branch (Optional):

```bash
# Local
git branch -d your-feature-branch

# Remote
git push origin --delete your-feature-branch
```

### 4. Original DAG Already Clean:

Since we created a **copy** instead of modifying the original, `my_dag.py` was never changed and is already in the correct state.

**Note:** The test DAG will automatically disappear from Airflow UI within 2-3 minutes after deleting from GCS.

---

## Important Notes

### ✅ DO:

- **Always create a test copy** - Never modify production DAG files for testing
- **Push branch to remote** - Airflow needs remote access to clone your code
- **Specify branch parameter** - Ensure your test DAG uses your feature branch
- **Clean up after testing** - Remove test DAG files and GCS uploads
- **Use dry-run flags** - Test scripts with `--dry-run` before actual execution
- **Verify in Code tab** - Check Airflow UI shows correct DAG version

### ❌ DON'T:

- **Don't modify original DAG** - Create a copy instead
- **Don't upload to production** - These scripts are for staging only
- **Don't skip branch push** - Airflow can't access unpushed branches
- **Don't assume branch defaults** - Always explicitly specify your test branch
- **Don't commit test DAG files** - They're temporary, clean them up after testing
- **Don't upload config files** - Only upload DAG files, not global configs like `crons.py`

### Branch Isolation Guarantees:

- ✅ Original DAG continues working on `master` branch
- ✅ Test DAG uses your feature branch
- ✅ Both DAGs can run simultaneously
- ✅ No risk of breaking production during testing
- ✅ Easy cleanup after testing

---

## Script Usage Reference

### create_test_dag.py

**Purpose:** Create a test copy of a DAG with modified dag_id

**Basic usage:**
```bash
./orchestration/scripts/create_test_dag.py <dag_file_path>
```

**Options:**
- `--suffix <name>` - Custom suffix instead of `_test_<username>`
- `--output <path>` - Custom output file path
- `--dry-run` - Preview changes without creating file

**Examples:**
```bash
# Auto-generate with username
./orchestration/scripts/create_test_dag.py orchestration/dags/path/to/my_dag.py

# Custom suffix
./orchestration/scripts/create_test_dag.py --suffix debug orchestration/dags/path/to/my_dag.py

# Custom output path
./orchestration/scripts/create_test_dag.py --output orchestration/dags/test/my_test.py orchestration/dags/path/to/my_dag.py

# Dry-run
./orchestration/scripts/create_test_dag.py --dry-run orchestration/dags/path/to/my_dag.py
```

---

### push_to_staging.sh

**Purpose:** Upload DAG files to GCS staging bucket

**Basic usage:**
```bash
./orchestration/scripts/push_to_staging.sh <dag_file_path>
```

**Options:**
- `--dry-run` - Preview upload command without executing

**Examples:**
```bash
# Upload from relative path
./orchestration/scripts/push_to_staging.sh orchestration/dags/path/to/my_dag_test_john.py

# Upload from absolute path
./orchestration/scripts/push_to_staging.sh /full/path/to/data-gcp/orchestration/dags/path/to/my_dag_test_john.py

# Dry-run
./orchestration/scripts/push_to_staging.sh --dry-run orchestration/dags/path/to/my_dag_test_john.py
```

---

## Complete Example Workflow

Let's walk through testing a modified DAG called `my_analysis_dag`:

```bash
# 1. Ensure changes are committed and pushed
git checkout -b feature/improve-analysis
git add jobs/etl_jobs/analytics/
git commit -m "Feature: Improve analysis logic"
git push origin feature/improve-analysis

# 2. Create test copy of DAG
./orchestration/scripts/create_test_dag.py orchestration/dags/jobs/analytics/my_analysis_dag.py
# Creates: my_analysis_dag_test_john.py

# 3. Upload test DAG to staging
./orchestration/scripts/push_to_staging.sh orchestration/dags/jobs/analytics/my_analysis_dag_test_john.py

# 4. Open Airflow Staging UI
# URL: https://airflow-stg.data.ehp.passculture.team/home

# 5. Wait 2-3 minutes, then search for "my_analysis_dag_test_john"

# 6. Trigger with branch config:
# {
#   "branch": "feature/improve-analysis"
# }

# 7. Monitor execution and verify results

# 8. After testing complete, clean up:
rm orchestration/dags/jobs/analytics/my_analysis_dag_test_john.py
gsutil rm gs://airflow-data-bucket-stg/dags/jobs/analytics/my_analysis_dag_test_john.py
```

---

## Additional Resources

- **Airflow Staging:** https://airflow-stg.data.ehp.passculture.team/home
- **GCS Bucket:** `gs://airflow-data-bucket-stg/dags/`
- **Scripts Location:** `orchestration/scripts/`

For questions or issues, consult your team's Airflow documentation or reach out to the data engineering team.
