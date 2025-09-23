# QPI Historical Data Migration Scripts

This folder contains migration scripts for migrating QPI historical data from the legacy bucket structure to dedicated seed data buckets.

## Overview

**Migration Path:**
- **From:** `gs://data-bucket-{env}/QPI_historical/`
- **To:** `gs://seed-data-bucket-{env}/historical/`

**Priority:** HIGH PRIORITY (Phase 2 - Cold Reference Data Migration)

## Files

### `migrate_qpi_historical.py`
Main migration script that copies all QPI historical data from the old bucket structure to the new dedicated seed data bucket.

**Features:**
- Dry-run mode by default for safety
- Comprehensive logging with timestamped log files
- File count verification and size reporting
- Uses `gcloud storage rsync` for efficient transfer
- Post-migration verification

**Usage:**
```bash
# Dry run (safe - shows what would be migrated)
python migrate_qpi_historical.py --env prod --dry-run

# Execute actual migration
python migrate_qpi_historical.py --env prod --execute

# Help
python migrate_qpi_historical.py --help
```

### `delete_old_qpi_historical.py`
Deletion script that safely removes the old QPI historical folder after successful migration.

**Safety Features:**
- Dry-run mode by default
- Creates backup manifest before deletion
- Verifies new bucket has migrated data
- Requires explicit confirmation text
- Multiple confirmation prompts

**Usage:**
```bash
# Dry run (safe - shows what would be deleted)
python delete_old_qpi_historical.py --env prod --dry-run

# Execute actual deletion (requires confirmation)
python delete_old_qpi_historical.py --env prod --confirm-delete

# Help
python delete_old_qpi_historical.py --help
```

## Migration Process

### Prerequisites
1. Ensure target bucket `seed-data-bucket-{env}` exists
2. Verify permissions for both source and target buckets
3. Install and configure `gcloud` CLI

### Step-by-Step Process

1. **Analysis (Dry Run)**
   ```bash
   python migrate_qpi_historical.py --env {env} --dry-run
   ```

2. **Execute Migration**
   ```bash
   python migrate_qpi_historical.py --env {env} --execute
   ```

3. **Verify Migration**
   - Check migration logs for errors
   - Verify files exist in new bucket: `gs://seed-data-bucket-{env}/historical/`
   - Test QPI import processes

4. **Update Code References**
   Update the QPI import job:
   ```python
   # In orchestration/dags/jobs/import/import_qpi.py
   # Change path from: gs://data-bucket-{env}/QPI_historical/
   # To: gs://seed-data-bucket-{env}/historical/
   ```

5. **Clean Up (Optional)**
   ```bash
   # Only after confirming migration success and code updates
   python delete_old_qpi_historical.py --env {env} --confirm-delete
   ```

## Dependencies

### Code References to Update
- **File:** `orchestration/dags/jobs/import/import_qpi.py`
- **Path:** `QPI_historical/qpi_answers_historical_*.parquet`
- **New Path:** `historical/qpi_answers_historical_*.parquet`
- **Destination:** BigQuery raw dataset

## Log Files

Both scripts create detailed log files with timestamps:
- `migrate_qpi_historical_{env}_{timestamp}.log`
- `delete_old_qpi_historical_{env}_{timestamp}.log`

## Error Handling

- Scripts use comprehensive error handling
- Failed operations are logged with detailed error messages
- Dry-run mode allows safe testing before execution
- Migration verification catches common issues

## Backup and Recovery

- Deletion script creates manifest files before deletion
- Original data remains in source bucket until explicitly deleted
- Parallel operation during transition periods for safety

## Data Characteristics

- **Pattern:** Cold Storage (Read-only Historical Data)
- **Write Access:** None (historical data only)
- **Read Access:** Infrequent access by QPI import job
- **File Format:** Parquet files (`qpi_answers_historical_*.parquet`)
- **Risk Level:** Low (single consumer, minimal active usage)
- **Cost Impact:** High (cold storage optimization benefits)

## Migration Benefits

- **Perfect Isolation:** Read-only historical data with no active writes
- **Cold Storage Optimization:** Accessed infrequently for historical context
- **Better Organization:** Consolidation with seed data structure
- **Cost Savings:** Immediate benefits from dedicated bucket lifecycle policies

## Notes

- This data is read-only historical QPI answers
- Single consumer makes migration low-risk
- Better suited for seed-data-bucket as reference data
- Consider implementing retention policies after migration
