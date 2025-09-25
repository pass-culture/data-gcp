# QPI Exports Migration Scripts

This folder contains migration scripts for migrating QPI export data from the legacy bucket structure to the dedicated export bucket.

## Overview

**Migration Path:**
- **From:** `gs://data-bucket-{env}/QPI_exports/`
- **To:** `gs://de-bigquery-data-export-{env}/qpi_exports/`

**Priority:** LOW PRIORITY (Phase 4+ - Hot Data Migration with External Dependencies)

## Files

### `migrate_qpi_exports.py`
Main migration script that copies all QPI export data from the old bucket structure to the new dedicated export bucket.

**Features:**
- Dry-run mode by default for safety
- Comprehensive logging with timestamped log files
- Date partition analysis for QPI export files
- JSONL file detection and verification
- Uses `gcloud storage rsync` for efficient transfer
- Post-migration verification with critical file checking
- Hot data optimization for minimal downtime

**Usage:**
```bash
# Dry run (safe - shows what would be migrated)
python migrate_qpi_exports.py --env prod --dry-run

# Execute actual migration
python migrate_qpi_exports.py --env prod --execute

# Help
python migrate_qpi_exports.py --help
```

### `delete_old_qpi_exports.py`
Deletion script that safely removes the old QPI export folder after successful migration.

**Safety Features:**
- Dry-run mode by default
- Migration verification before deletion
- Creates backup manifest before deletion
- Critical JSONL file detection and warnings
- Requires explicit confirmation text
- Multiple confirmation prompts

**Usage:**
```bash
# Dry run (safe - shows what would be deleted)
python delete_old_qpi_exports.py --env prod --dry-run

# Execute actual deletion (requires confirmation)
python delete_old_qpi_exports.py --env prod --confirm-delete

# Help
python delete_old_qpi_exports.py --help
```

## Migration Process

### Prerequisites
1. Ensure target bucket `de-bigquery-data-export-{env}` exists
2. Verify permissions for both source and target buckets
3. Install and configure `gcloud` CLI
4. **CRITICAL:** Coordinate with external QPI system and development team

### Step-by-Step Process

1. **Analysis (Dry Run)**
   ```bash
   python migrate_qpi_exports.py --env {env} --dry-run
   ```

2. **Execute Migration**
   ```bash
   python migrate_qpi_exports.py --env {env} --execute
   ```

3. **Verify Migration**
   - Check migration logs for errors
   - Verify files exist in new bucket: `gs://de-bigquery-data-export-{env}/qpi_exports/`
   - Confirm JSONL export files are present and accessible

4. **Update Code References**
   Update the QPI import DAG:

   **Primary Update - orchestration/dags/jobs/import/import_qpi.py:**
   ```python
   # Change path from:
   qpi_exports_path = f"gs://data-bucket-{env}/QPI_exports/"

   # To:
   qpi_exports_path = f"gs://de-bigquery-data-export-{env}/qpi_exports/"
   ```

5. **Test Import Pipeline**
   - Test daily QPI import process with new paths
   - Verify file presence detection works correctly
   - Run end-to-end import to BigQuery
   - Test with recent export files

6. **Clean Up (Optional)**
   ```bash
   # Only after confirming migration success and code updates
   python delete_old_qpi_exports.py --env {env} --confirm-delete
   ```

## Data Overview

### QPI Export Data Structure
The QPI exports contain daily external system exports:

**Data Pattern:**
- **Daily Exports:** `qpi_answers_{YYYYMMDD}/`
- **File Format:** `*.jsonl` files (JSON Lines)
- **Volume:** Daily accumulation of export data
- **Access Pattern:** Hot daily processing

**Key Files:**
- **`qpi_answers_{YYYYMMDD}/*.jsonl`** - Daily QPI export data (CRITICAL)
- Date-partitioned folder structure
- JSONL format for BigQuery import compatibility

**External System:**
- QPI system writes daily exports to this location
- External dependency requiring coordination

## Dependencies

### Code References to Update

**Primary Consumer:** `orchestration/dags/jobs/import/import_qpi.py`
```python
# Current implementation checks:
# QPI_exports/qpi_answers_{today}/ for file presence
# QPI_exports/qpi_answers_{ds_nodash}/*.jsonl for import

# Path updates needed:
# OLD: gs://data-bucket-{env}/QPI_exports/
# NEW: gs://de-bigquery-data-export-{env}/qpi_exports/
```

**Import Process:**
1. Daily file presence check
2. JSONL file import to BigQuery raw dataset
3. Downstream processing of QPI data

## Log Files

Both scripts create detailed log files with timestamps:
- `migrate_qpi_exports_{env}_{timestamp}.log`
- `delete_old_qpi_exports_{env}_{timestamp}.log`

## Error Handling

- Scripts use comprehensive error handling
- Failed operations are logged with detailed error messages
- Dry-run mode allows safe testing before execution
- Migration verification ensures JSONL files are present
- Date partition analysis helps identify data patterns

## Backup and Recovery

- Deletion script creates manifest files before deletion
- Original data remains in source bucket until explicitly deleted
- Critical JSONL file verification ensures export data integrity
- Rollback plan available for quick path changes

## Data Characteristics

- **Pattern:** Hot Data (Daily External Import Pipeline)
- **Write Access:** External QPI system writes daily exports
- **Read Access:** Daily processing by import_qpi.py DAG
- **File Types:** JSONL files in date-partitioned folders
- **Risk Level:** High (hot data, external dependencies, daily processing)
- **Cost Impact:** Lower (hot data doesn't benefit from cold storage tiers)

## Migration Benefits

- **Better Organization:** Dedicated export bucket for external system data
- **Hot Data Optimization:** Maintained performance for daily processing
- **Clear Separation:** External imports isolated from internal data
- **Pipeline Isolation:** Reduced blast radius for import failures
- **Maintenance Efficiency:** Easier monitoring and troubleshooting

## Testing Strategy

### Pre-Migration Testing
1. Run dry-run to analyze current export data
2. Verify date partition structure and JSONL files
3. Check file access patterns and sizes
4. Coordinate with external QPI system team

### Post-Migration Testing
1. Verify all JSONL files copied successfully
2. Test file presence detection from new location
3. Run complete import_qpi.py DAG end-to-end
4. Verify BigQuery data integrity
5. Monitor daily import performance

## Performance Considerations

- **Hot Data Access:** Daily imports require consistent performance
- **External Dependencies:** QPI system expects reliable access
- **Minimal Downtime:** Migration should not disrupt daily imports
- **Network Transfer:** Plan migration during low-activity periods
- **Immediate Testing:** Quick verification of import functionality

## Critical Warnings

⚠️ **CRITICAL IMPACT:** This migration affects hot daily import pipeline!

- Daily QPI imports depend on this data location
- External QPI system writes to this location daily
- Failed migration breaks daily data ingestion
- Code references MUST be updated before cleanup
- Coordinate with development team for timing
- Test import pipeline immediately after migration

## Rollback Plan

If migration issues occur:
1. **Immediate:** Revert code references to old paths
2. **Quick Test:** Verify import_qpi.py works with old paths
3. **External Coordination:** Ensure QPI system can access old location
4. **Re-migration:** Fix issues and re-run migration
5. **No Data Loss:** Original files remain available during transition

## Dependencies Alert

**Before cleanup, ensure these are updated and tested:**
1. ✅ `orchestration/dags/jobs/import/import_qpi.py` path updated
2. ✅ Daily QPI import process tested end-to-end
3. ✅ File presence detection verified with new paths
4. ✅ BigQuery import functionality confirmed
5. ✅ External QPI system coordination completed

## Notes

- This is hot data requiring careful coordination
- External system dependency requires advance planning
- Daily processing cannot be interrupted for extended periods
- Consider migrating during maintenance windows
- Monitor performance impact on daily imports
- Plan rollback procedures for quick recovery

## External System Coordination

**Required Coordination:**
1. **QPI System Team:** Notify of path changes (if applicable)
2. **Development Team:** Code update coordination
3. **Operations Team:** Monitor daily import success
4. **Data Team:** Verify downstream data integrity

**Timeline Considerations:**
- Plan migration during low-activity periods
- Allow buffer time for testing and verification
- Have rollback plan ready for immediate use
- Monitor system for several days post-migration
