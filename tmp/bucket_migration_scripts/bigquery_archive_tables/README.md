# BigQuery Archive Tables Migration Scripts

This folder contains migration scripts for migrating BigQuery archive tables data from the legacy bucket structure to dedicated archive buckets.

## Overview

**Migration Path:**
- **From:** `gs://data-bucket-{env}/bigquery_archive_tables/`
- **To:** `gs://legacy-archive-bucket-{env}/archive/`

**Priority:** HIGH PRIORITY (Phase 1 - Cold Storage Migration)

## Files

### `migrate_bigquery_archive_tables.py`
Main migration script that copies all BigQuery archive tables data from the old bucket structure to the new dedicated legacy archive bucket.

**Features:**
- Dry-run mode by default for safety
- Comprehensive logging with timestamped log files
- File count verification and size reporting
- Uses `gcloud storage rsync` for efficient transfer
- Post-migration verification

**Usage:**
```bash
# Dry run (safe - shows what would be migrated)
python migrate_bigquery_archive_tables.py --env prod --dry-run

# Execute actual migration
python migrate_bigquery_archive_tables.py --env prod --execute

# Help
python migrate_bigquery_archive_tables.py --help
```

### `delete_old_bigquery_archive_tables.py`
Deletion script that safely removes the old BigQuery archive tables folder after successful migration.

**Safety Features:**
- Dry-run mode by default
- Creates backup manifest before deletion
- Verifies new bucket has migrated data
- Requires explicit confirmation text
- Multiple confirmation prompts

**Usage:**
```bash
# Dry run (safe - shows what would be deleted)
python delete_old_bigquery_archive_tables.py --env prod --dry-run

# Execute actual deletion (requires confirmation)
python delete_old_bigquery_archive_tables.py --env prod --confirm-delete

# Help
python delete_old_bigquery_archive_tables.py --help
```

## Migration Process

### Prerequisites
1. Ensure target bucket `legacy-archive-bucket-{env}` exists
2. Verify permissions for both source and target buckets
3. Install and configure `gcloud` CLI

### Step-by-Step Process

1. **Analysis (Dry Run)**
   ```bash
   python migrate_bigquery_archive_tables.py --env {env} --dry-run
   ```

2. **Execute Migration**
   ```bash
   python migrate_bigquery_archive_tables.py --env {env} --execute
   ```

3. **Verify Migration**
   - Check migration logs for errors
   - Verify files exist in new bucket
   - Test any dependent processes

4. **Clean Up (Optional)**
   ```bash
   # Only after confirming migration success
   python delete_old_bigquery_archive_tables.py --env {env} --confirm-delete
   ```

## Log Files

Both scripts create detailed log files with timestamps:
- `migrate_bigquery_archive_tables_{env}_{timestamp}.log`
- `delete_old_bigquery_archive_tables_{env}_{timestamp}.log`

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

- **Pattern:** Legacy Archive System (Cold Storage)
- **Access Pattern:** Cold storage, potentially deprecated
- **Risk Level:** Minimal (legacy system with low active usage)
- **Cost Impact:** High (immediate coldline storage benefits)

## Notes

- BigQuery archive tables are legacy archives that may be deprecated
- This is an excellent candidate for cleanup after migration
- Consider validating usage before deletion
- Migration provides opportunity to review and potentially delete unused data
