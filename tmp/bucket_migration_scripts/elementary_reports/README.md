# Elementary Reports Migration Scripts

This folder contains migration scripts for migrating Elementary report data from the legacy bucket structure to the dedicated export bucket.

## Overview

**Migration Path:**
- **From:** `gs://data-bucket-{env}/elementary_reports/`
- **To:** `gs://de-bigquery-data-export-{env}/elementary_reports/`

**Priority:** LOW PRIORITY (Phase 4+ - Growing Volume with Retention Considerations)

## Files

### `migrate_elementary_reports.py`
Main migration script that copies all Elementary report data from the old bucket structure to the new dedicated export bucket.

**Features:**
- Dry-run mode by default for safety
- Comprehensive logging with timestamped log files
- Date-organized report analysis (year/month patterns)
- HTML report file detection and verification
- Uses `gcloud storage rsync` for efficient transfer
- Post-migration verification with report file checking
- Growing volume analysis and insights

**Usage:**
```bash
# Dry run (safe - shows what would be migrated)
python migrate_elementary_reports.py --env prod --dry-run

# Execute actual migration
python migrate_elementary_reports.py --env prod --execute

# Help
python migrate_elementary_reports.py --help
```

### `delete_old_elementary_reports.py`
Deletion script that safely removes the old Elementary reports folder after successful migration.

**Safety Features:**
- Dry-run mode by default
- Migration verification before deletion
- Creates backup manifest before deletion
- Report archive analysis and coverage summary
- Requires explicit confirmation text
- Multiple confirmation prompts

**Usage:**
```bash
# Dry run (safe - shows what would be deleted)
python delete_old_elementary_reports.py --env prod --dry-run

# Execute actual deletion (requires confirmation)
python delete_old_elementary_reports.py --env prod --confirm-delete

# Help
python delete_old_elementary_reports.py --help
```

## Migration Process

### Prerequisites
1. Ensure target bucket `de-bigquery-data-export-{env}` exists
2. Verify permissions for both source and target buckets
3. Install and configure `gcloud` CLI
4. **CONSIDER:** Plan retention policy implementation post-migration

### Step-by-Step Process

1. **Analysis (Dry Run)**
   ```bash
   python migrate_elementary_reports.py --env {env} --dry-run
   ```

2. **Execute Migration**
   ```bash
   python migrate_elementary_reports.py --env {env} --execute
   ```

3. **Verify Migration**
   - Check migration logs for errors
   - Verify files exist in new bucket: `gs://de-bigquery-data-export-{env}/elementary_reports/`
   - Confirm HTML report files are present and accessible

4. **Update Code References**
   Update the dbt artifacts DAG:

   **Primary Update - orchestration/dags/jobs/dbt/dbt_artifacts.py:**
   ```python
   # Change path from:
   elementary_reports_path = f"gs://data-bucket-{env}/elementary_reports/"

   # To:
   elementary_reports_path = f"gs://de-bigquery-data-export-{env}/elementary_reports/"
   ```

5. **Test Report Generation**
   - Test daily report generation with new paths
   - Verify HTML report creation and storage
   - Test report accessibility for manual viewing
   - Verify year-based folder organization

6. **Implement Retention Policy (Recommended)**
   Consider implementing retention policies for growing report volume:
   ```yaml
   # Example lifecycle policy for Elementary reports
   lifecycle:
     rule:
       - action:
           type: Delete
         condition:
           age: 365  # Delete reports older than 1 year
   ```

7. **Clean Up (Optional)**
   ```bash
   # Only after confirming migration success and code updates
   python delete_old_elementary_reports.py --env {env} --confirm-delete
   ```

## Data Overview

### Elementary Reports Structure
The Elementary reports contain daily data quality monitoring reports:

**Report Pattern:**
- **Daily Generation:** `orchestration/dags/jobs/dbt/dbt_artifacts.py`
- **File Pattern:** `elementary_report_{YYYYMMDD}.html`
- **Organization:** `elementary_reports/{year}/elementary_report_{YYYYMMDD}.html`
- **Volume Growth:** Daily accumulation requiring management

**Key Characteristics:**
- **File Type:** HTML reports for data quality monitoring
- **Access Pattern:** Manual access for report viewing (occasional)
- **Growth Pattern:** Daily accumulation creating volume management need
- **Retention Need:** Consider retention policy for old reports

## Dependencies

### Code References to Update

**Primary Consumer:** `orchestration/dags/jobs/dbt/dbt_artifacts.py`
```python
# Current implementation:
# Creates daily data quality monitoring reports
# Path: elementary_reports/{year}/elementary_report_{YYYYMMDD}.html

# Path updates needed:
# OLD: gs://data-bucket-{env}/elementary_reports/
# NEW: gs://de-bigquery-data-export-{env}/elementary_reports/
```

**Report Generation Process:**
1. Daily dbt artifacts processing
2. Elementary report HTML generation
3. Year-based folder organization
4. Manual report viewing for monitoring

## Log Files

Both scripts create detailed log files with timestamps:
- `migrate_elementary_reports_{env}_{timestamp}.log`
- `delete_old_elementary_reports_{env}_{timestamp}.log`

## Error Handling

- Scripts use comprehensive error handling
- Failed operations are logged with detailed error messages
- Dry-run mode allows safe testing before execution
- Migration verification ensures HTML files are present
- Date organization analysis helps identify report patterns

## Backup and Recovery

- Deletion script creates manifest files before deletion
- Original data remains in source bucket until explicitly deleted
- Report archive analysis ensures coverage continuity
- Rollback plan available for quick path changes

## Data Characteristics

- **Pattern:** Daily Generation with Growing Volume
- **Write Access:** Daily report generation by dbt_artifacts.py
- **Read Access:** Manual access for report viewing (occasional)
- **File Types:** HTML files organized by year
- **Risk Level:** Low (operational monitoring, not critical data)
- **Cost Impact:** Growing volume requires retention management

## Migration Benefits

- **Better Organization:** Dedicated export bucket for report archives
- **Growing Volume Management:** Foundation for retention policies
- **Clear Separation:** Report archives isolated from production data
- **Retention Optimization:** Easier implementation of lifecycle policies
- **Monitoring Efficiency:** Cleaner report archive organization

## Testing Strategy

### Pre-Migration Testing
1. Run dry-run to analyze current report archive
2. Verify HTML file patterns and year organization
3. Check report generation dependencies
4. Estimate archive size and growth patterns

### Post-Migration Testing
1. Verify all HTML reports copied successfully
2. Test report generation with new location
3. Verify year-based folder structure maintained
4. Test manual report access and viewing
5. Verify dbt artifacts DAG functionality

## Performance Considerations

- **Growing Volume:** Archive size increases daily
- **Retention Planning:** Old reports may not be accessed frequently
- **Storage Optimization:** Consider lifecycle policies post-migration
- **Access Patterns:** Occasional manual access, optimize for cost
- **Network Transfer:** Large archive migration may take time

## Retention Policy Recommendations

After successful migration, consider implementing retention policies:

### Example Retention Policy
```yaml
# Suggested lifecycle policy for Elementary reports
lifecycle:
  rule:
    - action:
        type: SetStorageClass
        storageClass: COLDLINE
      condition:
        age: 90  # Move to cold storage after 3 months
    - action:
        type: Delete
      condition:
        age: 365  # Delete reports older than 1 year
```

### Retention Considerations
- **Keep Recent:** Last 30-90 days for active monitoring
- **Archive Medium-term:** 3-12 months in cold storage
- **Delete Old:** Reports older than 1 year (configurable)
- **Critical Events:** Preserve reports during incident periods

## Critical Warnings

⚠️ **NOTE:** This migration affects daily report generation and growing volume!

- Daily report generation depends on this location
- Report archive grows continuously without retention
- Failed migration affects monitoring workflow
- Code references MUST be updated before cleanup
- Consider retention policy implementation
- Monitor storage costs post-migration

## Rollback Plan

If migration issues occur:
1. **Immediate:** Revert code references in dbt_artifacts.py
2. **Quick Test:** Verify report generation works with old paths
3. **Archive Verification:** Ensure report archive accessibility
4. **Re-migration:** Fix issues and re-run migration
5. **No Data Loss:** Original reports remain available

## Dependencies Alert

**Before cleanup, ensure these are updated and tested:**
1. ✅ `orchestration/dags/jobs/dbt/dbt_artifacts.py` path updated
2. ✅ Daily report generation tested with new bucket
3. ✅ HTML report creation and storage verified
4. ✅ Report accessibility confirmed for manual viewing
5. ✅ Year-based folder organization maintained

## Notes

- This affects daily report generation workflow
- Growing volume requires retention management
- Consider implementing lifecycle policies after migration
- Monitor storage costs for large report archives
- Plan retention strategy before archive grows too large
- Old reports may have limited access needs

## Post-Migration Recommendations

**Immediate Actions:**
1. **Test Report Generation:** Verify daily dbt artifacts workflow
2. **Verify Access:** Confirm manual report viewing works
3. **Monitor Volume:** Track daily report accumulation

**Long-term Actions:**
1. **Implement Retention:** Deploy lifecycle policies for cost optimization
2. **Monitor Costs:** Track storage costs for report archive
3. **Review Access Patterns:** Determine actual report access needs
4. **Optimize Storage:** Consider cold storage for old reports

**Maintenance:**
- Regular review of retention policy effectiveness
- Monitor report archive growth trends
- Periodic cleanup of unnecessary old reports
- Cost optimization through storage class transitions
