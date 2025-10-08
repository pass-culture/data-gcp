# Base32-Encode Utility Migration Scripts

This folder contains migration scripts for migrating base32-encode utility files from the legacy bucket structure to a dedicated shared utilities bucket.

## Overview

**Migration Path:**
- **From:** `gs://data-bucket-{env}/base32-encode/`
- **To:** `gs://shared-utilities-bucket-{env}/base32-encode/`

**Priority:** MEDIUM PRIORITY (Phase 3 - Warm Isolated Data Migration)

## Files

### `migrate_base32_utils.py`
Main migration script that copies all base32-encode utility files from the old bucket structure to the new dedicated shared utilities bucket.

**Features:**
- Dry-run mode by default for safety
- Comprehensive logging with timestamped log files
- File type analysis and key file detection
- Uses `gcloud storage rsync` for efficient transfer
- Post-migration verification with critical file checking
- Small file size optimization

**Usage:**
```bash
# Dry run (safe - shows what would be migrated)
python migrate_base32_utils.py --env prod --dry-run

# Execute actual migration
python migrate_base32_utils.py --env prod --execute

# Help
python migrate_base32_utils.py --help
```

### `delete_old_base32_utils.py`
Deletion script that safely removes the old base32-encode utility folder after successful migration.

**Safety Features:**
- Dry-run mode by default
- Migration verification before deletion
- Creates backup manifest before deletion
- Critical file detection and warnings
- Requires explicit confirmation text
- Multiple confirmation prompts

**Usage:**
```bash
# Dry run (safe - shows what would be deleted)
python delete_old_base32_utils.py --env prod --dry-run

# Execute actual deletion (requires confirmation)
python delete_old_base32_utils.py --env prod --confirm-delete

# Help
python delete_old_base32_utils.py --help
```

## Migration Process

### Prerequisites
1. Ensure target bucket `shared-utilities-bucket-{env}` exists
2. Verify permissions for both source and target buckets
3. Install and configure `gcloud` CLI
4. **CRITICAL:** Coordinate with BigQuery UDF consumers

### Step-by-Step Process

1. **Analysis (Dry Run)**
   ```bash
   python migrate_base32_utils.py --env {env} --dry-run
   ```

2. **Execute Migration**
   ```bash
   python migrate_base32_utils.py --env {env} --execute
   ```

3. **Verify Migration**
   - Check migration logs for errors
   - Verify files exist in new bucket: `gs://shared-utilities-bucket-{env}/base32-encode/`
   - Confirm `base32.js` is present and accessible

4. **Update Code References**
   Update the configuration and UDF references:

   **Primary Update - orchestration/dags/common/config.py:**
   ```python
   # Change path from:
   base32_path = f"gs://data-bucket-{env}/base32-encode/base32.js"

   # To:
   base32_path = f"gs://shared-utilities-bucket-{env}/base32-encode/base32.js"
   ```

   **Secondary Updates - BigQuery UDF Macros:**
   Update all UDF files in `orchestration/dags/data_gcp_dbt/macros/function_utils/udf_js/*.sql` that reference the base32 utility.

5. **Test BigQuery UDF Functions**
   - Test all UDF functions that use base32 encoding
   - Verify JavaScript functions load correctly
   - Run end-to-end tests for affected BigQuery operations

6. **Clean Up (Optional)**
   ```bash
   # Only after confirming migration success and code updates
   python delete_old_base32_utils.py --env {env} --confirm-delete
   ```

## Data Overview

### Base32-Encode Utilities
The base32-encode folder contains shared JavaScript utilities for BigQuery UDF functions:

**Key Files:**
- **`base32.js`** - Main base32 encoding/decoding utility (CRITICAL)
- Additional JavaScript helper files
- Configuration files (if any)

**Consumers:**
- Multiple BigQuery UDF JavaScript functions
- Referenced via `orchestration/dags/common/config.py`
- Used in `orchestration/dags/data_gcp_dbt/macros/function_utils/udf_js/*.sql`

## Dependencies

### Code References to Update

**Primary Consumer:** `orchestration/dags/common/config.py`
```python
# Current path configuration
DATA_GCS_BUCKET_NAME = f"data-bucket-{ENV_SHORT_NAME}"
# Used to construct: gs://data-bucket-{env}/base32-encode/base32.js
```

**BigQuery UDF Macros:** Files in `orchestration/dags/data_gcp_dbt/macros/function_utils/udf_js/`
- Any `.sql` files that create JavaScript UDFs using base32 utilities
- UDF definitions that load external JavaScript libraries

### Required Configuration Updates:
1. **config.py** - Update base32 utility path references
2. **UDF Macros** - Update JavaScript library loading paths
3. **dbt configurations** - Any macro variables pointing to base32 utilities

## Log Files

Both scripts create detailed log files with timestamps:
- `migrate_base32_utils_{env}_{timestamp}.log`
- `delete_old_base32_utils_{env}_{timestamp}.log`

## Error Handling

- Scripts use comprehensive error handling
- Failed operations are logged with detailed error messages
- Dry-run mode allows safe testing before execution
- Migration verification ensures critical files are present
- File type analysis helps identify unexpected files

## Backup and Recovery

- Deletion script creates manifest files before deletion
- Original data remains in source bucket until explicitly deleted
- Critical file verification ensures base32.js is successfully migrated
- Rollback plan available for quick path changes

## Data Characteristics

- **Pattern:** Shared Utility Library (Small, Multi-consumer)
- **Write Access:** Manual deployment (rare updates)
- **Read Access:** Frequent by BigQuery UDF functions
- **File Size:** Very small (KB range)
- **Risk Level:** Medium (multiple consumers, critical for UDF functions)
- **Cost Impact:** Low (small files, but better organization)

## Migration Benefits

- **Better Organization:** Dedicated utilities bucket for shared resources
- **Small File Size:** Easy to migrate with minimal risk
- **Infrastructure Improvement:** Clear separation of utilities from data
- **Multiple Consumer Support:** Centralized location for shared utilities
- **Maintenance Efficiency:** Easier utility updates and versioning

## Testing Strategy

### Pre-Migration Testing
1. Run dry-run to identify all utility files
2. Verify `base32.js` is present and analyze file types
3. Check current BigQuery UDF usage patterns
4. Estimate migration time (should be very fast)

### Post-Migration Testing
1. Verify all files copied successfully to new location
2. Test `base32.js` accessibility from new path
3. Run BigQuery UDF functions that use base32 encoding
4. Verify JavaScript UDF creation and execution
5. Test end-to-end workflows using base32 utilities

## Performance Considerations

- **Very Small Dataset:** Minimal transfer time and cost
- **High Availability Need:** UDF functions depend on utilities
- **Quick Migration:** Can be completed in minutes
- **Low Network Impact:** Small file sizes
- **Immediate Testing:** Quick verification of UDF functionality

## Critical Warnings

⚠️ **CRITICAL IMPACT:** This migration affects BigQuery UDF JavaScript functions!

- Multiple UDF functions depend on `base32.js`
- Failed migration breaks JavaScript UDFs
- Code references MUST be updated before cleanup
- Test all BigQuery functions after migration
- Have rollback plan ready for quick path reversion

## Rollback Plan

If migration issues occur:
1. **Immediate:** Revert code references to old paths
2. **Quick Test:** Verify UDF functions work with old paths
3. **Re-migration:** Fix issues and re-run migration
4. **No Data Loss:** Original files remain available during transition

## Dependencies Alert

**Before cleanup, ensure these are updated and tested:**
1. ✅ `orchestration/dags/common/config.py` path updated
2. ✅ All BigQuery UDF macros using base32 utilities tested
3. ✅ dbt compilation successful with new paths
4. ✅ JavaScript UDF creation and execution verified
5. ✅ End-to-end BigQuery workflows tested

## Notes

- This is the smallest migration in terms of data volume
- Critical for BigQuery JavaScript UDF functionality
- Multiple consumer coordination required
- Quick migration but thorough testing essential
- Consider implementing utility versioning after migration
