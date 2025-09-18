# ClickHouse Export Migration Scripts

This folder contains migration scripts for migrating ClickHouse export data from the legacy bucket structure to the dedicated export bucket.

## Overview

**Migration Path:**
- **From:** `gs://data-bucket-{env}/clickhouse_export/`
- **To:** `gs://de-bigquery-data-export-{env}/clickhouse_export/`

**Priority:** LOW PRIORITY (Phase 4+ - Active Daily Staging with External System Integration)

## Files

### `migrate_clickhouse_export.py`
Main migration script that copies all ClickHouse export staging data from the old bucket structure to the new dedicated export bucket.

**Features:**
- Dry-run mode by default for safety
- Comprehensive logging with timestamped log files
- Environment and date pattern analysis
- Export data file detection and verification
- Uses `gcloud storage rsync` for efficient transfer
- Post-migration verification with staging data checking
- External system integration optimization

**Usage:**
```bash
# Dry run (safe - shows what would be migrated)
python migrate_clickhouse_export.py --env prod --dry-run

# Execute actual migration
python migrate_clickhouse_export.py --env prod --execute

# Help
python migrate_clickhouse_export.py --help
```

### `delete_old_clickhouse_export.py`
Deletion script that safely removes the old ClickHouse export folder after successful migration.

**Safety Features:**
- Dry-run mode by default
- Migration verification before deletion
- Creates backup manifest before deletion
- Critical staging data file detection and warnings
- Export pattern analysis and coverage summary
- Requires explicit confirmation text
- Multiple confirmation prompts

**Usage:**
```bash
# Dry run (safe - shows what would be deleted)
python delete_old_clickhouse_export.py --env prod --dry-run

# Execute actual deletion (requires confirmation)
python delete_old_clickhouse_export.py --env prod --confirm-delete

# Help
python delete_old_clickhouse_export.py --help
```

## Migration Process

### Prerequisites
1. Ensure target bucket `de-bigquery-data-export-{env}` exists
2. Verify permissions for both source and target buckets
3. Install and configure `gcloud` CLI
4. **CRITICAL:** Coordinate with external ClickHouse system and development team

### Step-by-Step Process

1. **Analysis (Dry Run)**
   ```bash
   python migrate_clickhouse_export.py --env {env} --dry-run
   ```

2. **Execute Migration**
   ```bash
   python migrate_clickhouse_export.py --env {env} --execute
   ```

3. **Verify Migration**
   - Check migration logs for errors
   - Verify files exist in new bucket: `gs://de-bigquery-data-export-{env}/clickhouse_export/`
   - Confirm staging data files are present and accessible

4. **Update Code References**
   Update the ClickHouse export DAG:

   **Primary Update - orchestration/dags/jobs/export/export_clickhouse.py:**
   ```python
   # Change path from:
   clickhouse_export_path = f"gs://data-bucket-{env}/clickhouse_export/"

   # To:
   clickhouse_export_path = f"gs://de-bigquery-data-export-{env}/clickhouse_export/"
   ```

5. **Test Export Pipeline**
   - Test complete BigQuery → GCS → ClickHouse pipeline
   - Verify staging data export functionality
   - Test external ClickHouse integration
   - Run end-to-end export workflow

6. **Clean Up (Optional)**
   ```bash
   # Only after confirming migration success and code updates
   python delete_old_clickhouse_export.py --env {env} --confirm-delete
   ```

## Data Overview

### ClickHouse Export Staging Pipeline
The ClickHouse export contains active staging data for external system integration:

**Export Flow:**
- **Data Export:** `orchestration/dags/jobs/export/export_clickhouse.py`
- **Staging Pattern:** `{DATA_GCS_BUCKET_NAME}/clickhouse_export/{ENV_SHORT_NAME}/export/{DATE}`
- **External Integration:** Forwards staged data to ClickHouse
- **Pipeline:** BigQuery → GCS staging → ClickHouse

**Key Files:**
- **Export Data Files:** CSV, Parquet, JSON files with BigQuery data extracts
- **Compressed Files:** .gz compressed exports for efficient transfer
- **Date-Organized:** Daily export operations in date-based folders
- **Temporary Staging:** Data staged for ClickHouse consumption

## Dependencies

### Code References to Update

**Primary Consumer:** `orchestration/dags/jobs/export/export_clickhouse.py`
```python
# Current implementation:
# Exports BigQuery data to GCS as intermediate staging
# Forwards staged data to ClickHouse daily
# Path pattern: {DATA_GCS_BUCKET_NAME}/clickhouse_export/{ENV_SHORT_NAME}/export/{DATE}

# Path updates needed:
# OLD: gs://data-bucket-{env}/clickhouse_export/
# NEW: gs://de-bigquery-data-export-{env}/clickhouse_export/
```

**Export Process:**
1. BigQuery data extraction to GCS staging
2. Date-based folder organization
3. External ClickHouse integration
4. Temporary staging data lifecycle

## Log Files

Both scripts create detailed log files with timestamps:
- `migrate_clickhouse_export_{env}_{timestamp}.log`
- `delete_old_clickhouse_export_{env}_{timestamp}.log`

## Error Handling

- Scripts use comprehensive error handling
- Failed operations are logged with detailed error messages
- Dry-run mode allows safe testing before execution
- Migration verification ensures staging files are present
- Export pattern analysis helps identify data organization

## Backup and Recovery

- Deletion script creates manifest files before deletion
- Original data remains in source bucket until explicitly deleted
- Critical staging file verification ensures pipeline integrity
- Rollback plan available for immediate pipeline restoration

## Data Characteristics

- **Pattern:** Active Daily Staging (Temporary Data with External System Integration)
- **Write Access:** Daily staging by export_clickhouse.py DAG
- **Read Access:** External ClickHouse system consumption
- **File Types:** CSV, Parquet, JSON, compressed files in date-based folders
- **Risk Level:** Medium (active staging, external dependencies, temporary data)
- **Cost Impact:** Lower (temporary staging data, limited retention needs)

## Migration Benefits

- **Better Organization:** Dedicated export bucket for staging data
- **Pipeline Isolation:** Export failures don't affect other data
- **External System Clarity:** Clear staging boundaries for ClickHouse
- **Temporary Data Management:** Better lifecycle management for staging
- **Troubleshooting:** Easier pipeline monitoring and debugging

## Testing Strategy

### Pre-Migration Testing
1. Run dry-run to analyze current staging data
2. Verify export patterns and staging file types
3. Check external ClickHouse integration dependencies
4. Coordinate with development team and external system

### Post-Migration Testing
1. Verify all staging files copied successfully
2. Test BigQuery data export to new location
3. Test external ClickHouse integration
4. Run complete export pipeline end-to-end
5. Verify staging data consumption by ClickHouse
6. Monitor export performance and external system integration

## Performance Considerations

- **Temporary Staging Data:** Short-term data lifecycle
- **External System Integration:** ClickHouse dependency complexity
- **Daily Processing:** Regular staging pattern requires consistency
- **Network Transfer:** Plan migration during low export activity
- **Immediate Testing:** Quick verification of external system connectivity

## Critical Warnings

⚠️ **CRITICAL IMPACT:** This migration affects active daily staging pipeline!

- Daily staging pattern for external ClickHouse integration
- External system dependency requiring coordination
- Failed migration breaks BigQuery → ClickHouse data flow
- Code references MUST be updated before cleanup
- Test external system integration before cleanup
- Monitor pipeline performance after migration

## Rollback Plan

If migration issues occur:
1. **Immediate:** Revert code references in export_clickhouse.py
2. **Quick Test:** Verify export pipeline works with old paths
3. **External Integration:** Test ClickHouse connectivity with old staging
4. **Re-migration:** Fix issues and re-run migration
5. **No Data Loss:** Original staging data remains available

## Dependencies Alert

**Before cleanup, ensure these are updated and tested:**
1. ✅ `orchestration/dags/jobs/export/export_clickhouse.py` path updated
2. ✅ Complete export pipeline tested end-to-end
3. ✅ BigQuery → GCS staging functionality verified
4. ✅ External ClickHouse integration confirmed
5. ✅ Daily export workflow tested

## Notes

- This affects active daily staging pipeline
- External system integration requires careful coordination
- Temporary staging data has different lifecycle needs
- Consider retention policies for staging data cleanup
- Monitor external system performance after migration
- Plan rollback procedures for immediate use

## External System Coordination

**Required Coordination:**
1. **Development Team:** Code update synchronization
2. **Data Engineering:** Export pipeline verification
3. **Operations Team:** Monitor export success rates
4. **External System Team:** ClickHouse integration testing

**Timeline Considerations:**
- Plan migration during low export activity
- Allow buffer time for external system testing
- Have rollback plan ready for immediate use
- Monitor export pipeline for several cycles
- Verify data availability in ClickHouse

## Staging Data Lifecycle

**Staging Characteristics:**
- **Temporary Data:** Short retention period
- **External Consumption:** ClickHouse system dependency
- **Daily Refresh:** Regular staging pattern
- **Performance Critical:** External system expectations

**Migration Considerations:**
- **Minimal Disruption:** External system cannot be interrupted
- **Quick Verification:** Immediate testing of connectivity
- **Retention Management:** Consider cleanup policies for staging
- **Performance Monitoring:** External system integration metrics

## Temporary Data Management

After successful migration, consider implementing:

### Staging Data Retention Policy
```yaml
# Example lifecycle policy for temporary staging data
lifecycle:
  rule:
    - action:
        type: Delete
      condition:
        age: 7  # Delete staging data older than 7 days
```

### Benefits of Retention Policy
- **Cost Optimization:** Remove unnecessary staging data
- **Storage Efficiency:** Temporary data lifecycle management
- **Performance:** Reduce bucket size for active operations
- **Compliance:** Clear data retention boundaries
