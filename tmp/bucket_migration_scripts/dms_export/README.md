# DMS Export Migration Scripts

This folder contains migration scripts for migrating DMS export data from the legacy bucket structure to the dedicated export bucket.

## Overview

**Migration Path:**
- **From:** `gs://data-bucket-{env}/dms_export/`
- **To:** `gs://de-bigquery-data-export-{env}/dms_export/`

**Priority:** LOW PRIORITY (Phase 4+ - Hot Active Processing Migration)

## Files

### `migrate_dms_export.py`
Main migration script that copies all DMS export staging data from the old bucket structure to the new dedicated export bucket.

**Features:**
- Dry-run mode by default for safety
- Comprehensive logging with timestamped log files
- Staging pipeline file analysis (JSON → Parquet flow)
- File type detection and verification
- Uses `gcloud storage rsync` for efficient transfer
- Post-migration verification with staging file checking
- Hot data optimization for active ETL pipelines

**Usage:**
```bash
# Dry run (safe - shows what would be migrated)
python migrate_dms_export.py --env prod --dry-run

# Execute actual migration
python migrate_dms_export.py --env prod --execute

# Help
python migrate_dms_export.py --help
```

### `delete_old_dms_export.py`
Deletion script that safely removes the old DMS export folder after successful migration.

**Safety Features:**
- Dry-run mode by default
- Migration verification before deletion
- Creates backup manifest before deletion
- Critical staging file detection and warnings
- Requires explicit confirmation text
- Multiple confirmation prompts

**Usage:**
```bash
# Dry run (safe - shows what would be deleted)
python delete_old_dms_export.py --env prod --dry-run

# Execute actual deletion (requires confirmation)
python delete_old_dms_export.py --env prod --confirm-delete

# Help
python delete_old_dms_export.py --help
```

## Migration Process

### Prerequisites
1. Ensure target bucket `de-bigquery-data-export-{env}` exists
2. Verify permissions for both source and target buckets
3. Install and configure `gcloud` CLI
4. **CRITICAL:** Coordinate with development team for ETL pipeline timing

### Step-by-Step Process

1. **Analysis (Dry Run)**
   ```bash
   python migrate_dms_export.py --env {env} --dry-run
   ```

2. **Execute Migration**
   ```bash
   python migrate_dms_export.py --env {env} --execute
   ```

3. **Verify Migration**
   - Check migration logs for errors
   - Verify files exist in new bucket: `gs://de-bigquery-data-export-{env}/dms_export/`
   - Confirm JSON and Parquet staging files are present

4. **Update Code References**
   Update all DMS ETL components:

   **Primary Updates:**
   ```python
   # In jobs/etl_jobs/external/dms/main.py
   # Change path from:
   dms_output_path = f"gs://data-bucket-{env}/dms_export/"

   # To:
   dms_output_path = f"gs://de-bigquery-data-export-{env}/dms_export/"

   # In jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py
   # Update input/output paths similarly

   # In orchestration/dags/jobs/import/import_dms_subscriptions.py
   # Update processing paths
   ```

5. **Test ETL Pipeline**
   - Test complete DMS ETL flow: JSON → Parquet → BigQuery
   - Verify file processing from new locations
   - Run end-to-end DMS subscription import
   - Test daily processing workflow

6. **Clean Up (Optional)**
   ```bash
   # Only after confirming migration success and code updates
   python delete_old_dms_export.py --env {env} --confirm-delete
   ```

## Data Overview

### DMS Export Staging Pipeline
The DMS export contains active ETL staging data:

**ETL Flow:**
1. **Raw Data:** `jobs/etl_jobs/external/dms/main.py` writes JSON
   - Pattern: `unsorted_dms_{target}_{updated_since}.json`
2. **Processing:** `jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py` converts to Parquet
   - Pattern: `dms_jeunes_{updated_since}.parquet`, `dms_pro_{updated_since}.parquet`
3. **Import:** `orchestration/dags/jobs/import/import_dms_subscriptions.py` loads to BigQuery

**Key Files:**
- **JSON Files:** Raw DMS subscription data (staging input)
- **Parquet Files:** Processed tabular data (staging output)
- **Multi-step Pipeline:** Active transformation workflow

## Dependencies

### Code References to Update

**ETL Pipeline Components:**
1. **Data Extraction:** `jobs/etl_jobs/external/dms/main.py`
   - Writes JSON staging files to dms_export/
2. **Data Transformation:** `jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py`
   - Converts JSON to Parquet in same location
3. **Data Loading:** `orchestration/dags/jobs/import/import_dms_subscriptions.py`
   - Processes Parquet files for BigQuery import

**Path Updates Required:**
```python
# All three components need path updates:
# OLD: gs://data-bucket-{env}/dms_export/
# NEW: gs://de-bigquery-data-export-{env}/dms_export/
```

## Log Files

Both scripts create detailed log files with timestamps:
- `migrate_dms_export_{env}_{timestamp}.log`
- `delete_old_dms_export_{env}_{timestamp}.log`

## Error Handling

- Scripts use comprehensive error handling
- Failed operations are logged with detailed error messages
- Dry-run mode allows safe testing before execution
- Migration verification ensures staging files are present
- ETL pipeline file analysis helps identify processing patterns

## Backup and Recovery

- Deletion script creates manifest files before deletion
- Original data remains in source bucket until explicitly deleted
- Critical staging file verification ensures ETL integrity
- Rollback plan available for immediate ETL restoration

## Data Characteristics

- **Pattern:** Hot Active Processing (ETL Staging Pipeline)
- **Write Access:** Multiple ETL jobs write to staging location
- **Read Access:** Daily processing by import DAG
- **File Types:** JSON (raw) and Parquet (processed) staging files
- **Risk Level:** High (active ETL pipeline, multi-step processing)
- **Cost Impact:** Lower (hot data, frequent access, temporary storage)

## Migration Benefits

- **Better Organization:** Dedicated export bucket for ETL staging
- **Pipeline Isolation:** ETL failures don't affect other data
- **Hot Data Optimization:** Maintained performance for active processing
- **Clear Boundaries:** Staging data separated from production data
- **Troubleshooting:** Easier ETL pipeline monitoring and debugging

## Testing Strategy

### Pre-Migration Testing
1. Run dry-run to analyze current staging data
2. Verify JSON and Parquet file patterns
3. Check ETL pipeline dependencies
4. Coordinate with development team for timing

### Post-Migration Testing
1. Verify all staging files copied successfully
2. Test JSON file processing from new location
3. Test Parquet file generation and access
4. Run complete DMS ETL pipeline end-to-end
5. Verify BigQuery import functionality
6. Monitor ETL performance and error rates

## Performance Considerations

- **Hot Data Access:** ETL processing requires consistent performance
- **Multi-step Pipeline:** Each step depends on the previous stage
- **Minimal Downtime:** Migration should not disrupt active ETL
- **Network Transfer:** Plan migration during low ETL activity
- **Immediate Testing:** Quick verification of all ETL components

## Critical Warnings

⚠️ **CRITICAL IMPACT:** This migration affects active ETL staging pipeline!

- Multi-step ETL process depends on this staging location
- Daily DMS subscription processing uses these files
- Failed migration breaks ETL data flow
- All three ETL components need path updates
- Code references MUST be updated before cleanup
- Test complete ETL flow before cleanup
- Monitor ETL performance after migration

## Rollback Plan

If migration issues occur:
1. **Immediate:** Revert code references in all three ETL components
2. **Quick Test:** Verify ETL pipeline works with old paths
3. **Pipeline Verification:** Test JSON → Parquet → BigQuery flow
4. **Re-migration:** Fix issues and re-run migration
5. **No Data Loss:** Original staging files remain available

## Dependencies Alert

**Before cleanup, ensure these are updated and tested:**
1. ✅ `jobs/etl_jobs/external/dms/main.py` path updated
2. ✅ `jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py` path updated
3. ✅ `orchestration/dags/jobs/import/import_dms_subscriptions.py` path updated
4. ✅ Complete ETL flow tested end-to-end
5. ✅ JSON → Parquet → BigQuery verified

## Notes

- This affects active ETL staging pipeline
- Multi-step processing requires careful coordination
- All ETL components need simultaneous path updates
- Consider migrating during ETL maintenance windows
- Monitor ETL performance closely after migration
- Plan rollback procedures for immediate use

## ETL Pipeline Coordination

**Required Coordination:**
1. **Development Team:** Code update synchronization
2. **Data Engineering:** ETL flow verification
3. **Operations Team:** Monitor ETL success rates
4. **QA Team:** Verify data integrity post-migration

**Timeline Considerations:**
- Plan migration during low ETL activity
- Allow buffer time for comprehensive testing
- Have rollback plan ready for immediate use
- Monitor ETL pipeline for several processing cycles
- Verify data quality in downstream systems
