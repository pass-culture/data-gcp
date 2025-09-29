# CloudSQL Recommendation Sync Migration Scripts

This folder contains migration scripts for migrating CloudSQL recommendation sync data from the legacy bucket structure to the dedicated export bucket.

## Overview

**Migration Path:**
- **From:** `gs://data-bucket-{env}/export/cloudsql_recommendation_tables_to_bigquery/`
- **To:** `gs://de-bigquery-data-export-{env}/export_cloudsql_recommendation/`

**Priority:** LOW PRIORITY (Phase 4+ - Active Sync Operations Migration)

## Files

### `migrate_recommendation_sync.py`
Main migration script that copies all CloudSQL recommendation sync data from the old bucket structure to the new dedicated export bucket.

**Features:**
- Dry-run mode by default for safety
- Comprehensive logging with timestamped log files
- Timestamp-based sync operation analysis
- Sync data file detection and verification
- Uses `gcloud storage rsync` for efficient transfer
- Post-migration verification with sync data checking
- Bidirectional sync coordination optimization

**Usage:**
```bash
# Dry run (safe - shows what would be migrated)
python migrate_recommendation_sync.py --env prod --dry-run

# Execute actual migration
python migrate_recommendation_sync.py --env prod --execute

# Help
python migrate_recommendation_sync.py --help
```

### `delete_old_recommendation_sync.py`
Deletion script that safely removes the old recommendation sync folder after successful migration.

**Safety Features:**
- Dry-run mode by default
- Migration verification before deletion
- Creates backup manifest before deletion
- Critical sync data file detection and warnings
- Timestamp-based sync operation analysis
- Requires explicit confirmation text
- Multiple confirmation prompts

**Usage:**
```bash
# Dry run (safe - shows what would be deleted)
python delete_old_recommendation_sync.py --env prod --dry-run

# Execute actual deletion (requires confirmation)
python delete_old_recommendation_sync.py --env prod --confirm-delete

# Help
python delete_old_recommendation_sync.py --help
```

## Migration Process

### Prerequisites
1. Ensure target bucket `de-bigquery-data-export-{env}` exists
2. Verify permissions for both source and target buckets
3. Install and configure `gcloud` CLI
4. **CRITICAL:** Coordinate with development team for sync operations timing

### Step-by-Step Process

1. **Analysis (Dry Run)**
   ```bash
   python migrate_recommendation_sync.py --env {env} --dry-run
   ```

2. **Execute Migration**
   ```bash
   python migrate_recommendation_sync.py --env {env} --execute
   ```

3. **Verify Migration**
   - Check migration logs for errors
   - Verify files exist in new bucket: `gs://de-bigquery-data-export-{env}/export_cloudsql_recommendation/`
   - Confirm sync data files are present and accessible

4. **Update Code References**
   Update the bidirectional sync DAG:

   **Primary Update - orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py:**
   ```python
   # Change path from:
   sync_export_path = f"gs://data-bucket-{env}/export/cloudsql_recommendation_tables_to_bigquery/"

   # To:
   sync_export_path = f"gs://de-bigquery-data-export-{env}/export_cloudsql_recommendation/"
   ```

5. **Test Sync Operations**
   - Test complete bidirectional sync workflow
   - Verify BigQuery → GCS export functionality
   - Test GCS → CloudSQL synchronization
   - Verify sync coordination and timing

6. **Clean Up (Optional)**
   ```bash
   # Only after confirming migration success and code updates
   python delete_old_recommendation_sync.py --env {env} --confirm-delete
   ```

## Data Overview

### CloudSQL Recommendation Sync Structure
The recommendation sync contains bidirectional synchronization data:

**Sync Pattern:**
- **Export Operation:** `orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py`
- **Data Flow:** BigQuery → GCS → CloudSQL (bidirectional)
- **File Pattern:** Timestamp-based folders with sync data
- **Coordination:** Multiple coordinated operations within single use case

**Key Files:**
- **Sync Data Files:** CSV, Parquet, JSON files with recommendation data
- **Timestamp Folders:** Time-based organization for sync operations
- **Coordination Files:** Files managing bidirectional sync state

## Dependencies

### Code References to Update

**Primary Consumer:** `orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py`
```python
# Current implementation:
# Exports BigQuery data → GCS staging
# Manages bidirectional sync operations
# Path: export/cloudsql_recommendation_tables_to_bigquery/{timestamp}/

# Path updates needed:
# OLD: gs://data-bucket-{env}/export/cloudsql_recommendation_tables_to_bigquery/
# NEW: gs://de-bigquery-data-export-{env}/export_cloudsql_recommendation/
```

**Sync Process:**
1. BigQuery data export to GCS staging
2. Bidirectional sync coordination
3. CloudSQL synchronization operations
4. Sync state management and cleanup

## Log Files

Both scripts create detailed log files with timestamps:
- `migrate_recommendation_sync_{env}_{timestamp}.log`
- `delete_old_recommendation_sync_{env}_{timestamp}.log`

## Error Handling

- Scripts use comprehensive error handling
- Failed operations are logged with detailed error messages
- Dry-run mode allows safe testing before execution
- Migration verification ensures sync data files are present
- Timestamp analysis helps identify sync operation patterns

## Backup and Recovery

- Deletion script creates manifest files before deletion
- Original data remains in source bucket until explicitly deleted
- Critical sync data verification ensures operation continuity
- Rollback plan available for quick sync restoration

## Data Characteristics

- **Pattern:** Active Bidirectional Sync Operations
- **Write Access:** Daily sync operations by sync DAG
- **Read Access:** Bidirectional sync processing
- **File Types:** CSV, Parquet, JSON files in timestamp-based folders
- **Risk Level:** Medium (active sync operations, complex coordination)
- **Cost Impact:** Lower (active staging data, limited cold storage benefits)

## Migration Benefits

- **Better Organization:** Dedicated export bucket for sync operations
- **Sync Isolation:** Sync failures don't affect other data
- **Clear Boundaries:** Sync data separated from other exports
- **Coordination Efficiency:** Easier sync operation monitoring
- **Troubleshooting:** Simplified bidirectional sync debugging

## Testing Strategy

### Pre-Migration Testing
1. Run dry-run to analyze current sync data
2. Verify timestamp patterns and sync file types
3. Check bidirectional sync dependencies
4. Coordinate with development team for timing

### Post-Migration Testing
1. Verify all sync data files copied successfully
2. Test BigQuery → GCS export with new location
3. Test bidirectional sync coordination
4. Run complete sync workflow end-to-end
5. Verify CloudSQL synchronization functionality
6. Monitor sync operation performance

## Performance Considerations

- **Active Sync Operations:** Regular bidirectional processing
- **Complex Coordination:** Multiple coordinated operations
- **Minimal Downtime:** Migration should not disrupt sync operations
- **Network Transfer:** Plan migration during low sync activity
- **Immediate Testing:** Quick verification of sync functionality

## Critical Warnings

⚠️ **CRITICAL IMPACT:** This migration affects active bidirectional sync operations!

- Bidirectional sync between BigQuery and CloudSQL depends on this location
- Complex coordination operations manage sync state
- Failed migration breaks recommendation synchronization
- Code references MUST be updated before cleanup
- Test complete sync workflow before cleanup
- Monitor sync performance after migration

## Rollback Plan

If migration issues occur:
1. **Immediate:** Revert code references in sync DAG
2. **Quick Test:** Verify sync operations work with old paths
3. **Sync Verification:** Test bidirectional coordination
4. **Re-migration:** Fix issues and re-run migration
5. **No Data Loss:** Original sync data remains available

## Dependencies Alert

**Before cleanup, ensure these are updated and tested:**
1. ✅ `orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py` path updated
2. ✅ Complete bidirectional sync tested end-to-end
3. ✅ BigQuery → GCS export functionality verified
4. ✅ CloudSQL synchronization confirmed
5. ✅ Sync coordination operations tested

## Notes

- This affects active bidirectional sync operations
- Complex coordination requires careful timing
- All sync components need simultaneous path updates
- Consider migrating during sync maintenance windows
- Monitor sync performance closely after migration
- Plan rollback procedures for immediate use

## Sync Operation Coordination

**Required Coordination:**
1. **Development Team:** Code update synchronization
2. **Data Engineering:** Sync operation verification
3. **Operations Team:** Monitor sync success rates
4. **Database Team:** Verify CloudSQL synchronization

**Timeline Considerations:**
- Plan migration during low sync activity
- Allow buffer time for comprehensive sync testing
- Have rollback plan ready for immediate use
- Monitor sync operations for several cycles
- Verify data consistency in CloudSQL

## Bidirectional Sync Complexity

**Sync Components:**
- **BigQuery Export:** Data extraction to GCS staging
- **Sync Coordination:** State management and timing
- **CloudSQL Import:** Data synchronization to database
- **Bidirectional Flow:** Two-way data consistency

**Migration Considerations:**
- **Atomic Updates:** All components need simultaneous path changes
- **State Consistency:** Sync state must remain intact
- **Timing Coordination:** Minimize disruption to sync cycles
- **Rollback Speed:** Quick recovery for sync failures
