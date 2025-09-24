# BigQuery Imports Seed Data Migration Scripts

This folder contains migration scripts for migrating seed data from the legacy bucket structure to dedicated seed data buckets.

## Overview

**Migration Path:**
- **From:** `gs://data-bucket-{env}/bigquery_imports/seed/`
- **To:** `gs://seed-data-bucket-{env}/`

**Priority:** MEDIUM PRIORITY (Phase 3 - Warm Isolated Data Migration)

## Files

### `migrate_seed_data.py`
Main migration script that copies all seed reference data from the old bucket structure to the new dedicated seed data bucket.

**Features:**
- Dry-run mode by default for safety
- Comprehensive logging with timestamped log files
- Subfolder analysis with file count and size reporting
- Uses `gcloud storage rsync` for efficient recursive transfer
- Post-migration verification
- Detailed migration statistics per subfolder

**Usage:**
```bash
# Dry run (safe - shows what would be migrated)
python migrate_seed_data.py --env prod --dry-run

# Execute actual migration
python migrate_seed_data.py --env prod --execute

# Help
python migrate_seed_data.py --help
```

### `delete_old_seed_data.py`
Deletion script that safely removes the old seed data folder after successful migration.

**Safety Features:**
- Dry-run mode by default
- Migration verification before deletion
- Creates backup manifest before deletion
- Requires explicit confirmation text
- Multiple confirmation prompts

**Usage:**
```bash
# Dry run (safe - shows what would be deleted)
python delete_old_seed_data.py --env prod --dry-run

# Execute actual deletion (requires confirmation)
python delete_old_seed_data.py --env prod --confirm-delete

# Help
python delete_old_seed_data.py --help
```

## Migration Process

### Prerequisites
1. Ensure target bucket `seed-data-bucket-{env}` exists
2. Verify permissions for both source and target buckets
3. Install and configure `gcloud` CLI
4. Coordinate with data team (warm access pattern)

### Step-by-Step Process

1. **Analysis (Dry Run)**
   ```bash
   python migrate_seed_data.py --env {env} --dry-run
   ```

2. **Execute Migration**
   ```bash
   python migrate_seed_data.py --env {env} --execute
   ```

3. **Verify Migration**
   - Check migration logs for errors
   - Verify files exist in new bucket: `gs://seed-data-bucket-{env}/`
   - Test seed data import processes

4. **Update Code References**
   Update the GCS seed job:
   ```python
   # In jobs/etl_jobs/internal/gcs_seed/main.py
   # Change path from: gs://data-bucket-{env}/bigquery_imports/seed/
   # To: gs://seed-data-bucket-{env}/
   ```

5. **Test Applications**
   - Run seed data import job
   - Verify BigQuery dataset `seed_{ENV_SHORT_NAME}` is populated correctly
   - Test downstream processes that depend on seed data

6. **Clean Up (Optional)**
   ```bash
   # Only after confirming migration success and code updates
   python delete_old_seed_data.py --env {env} --confirm-delete
   ```

## Data Overview

### Seed Data Structure
The seed data contains **50+ reference tables** organized in subfolders:

**Geographic Data:**
- Country and region mappings
- Postal code references
- Administrative boundaries

**Institution Data:**
- Educational institution mappings
- Cultural venue references
- Partner organization data

**Business Data:**
- Category classifications
- Product and service mappings
- Compliance references

**File Formats:**
- CSV files (most common)
- Parquet files (structured data)
- Avro files (schema evolution)

## Dependencies

### Code References to Update
- **Primary Consumer:** `jobs/etl_jobs/internal/gcs_seed/main.py`
- **Configuration:** `REF_TABLES` mapping in the main script
- **Destination:** BigQuery dataset `seed_{ENV_SHORT_NAME}`
- **Path Pattern:** `bigquery_imports/seed/{subfolder}/` → `{subfolder}/`

### Update Required in gcs_seed/main.py:
```python
# OLD PATH
bucket_path = f"gs://data-bucket-{env}/bigquery_imports/seed/"

# NEW PATH
bucket_path = f"gs://seed-data-bucket-{env}/"
```

## Log Files

Both scripts create detailed log files with timestamps:
- `migrate_seed_data_{env}_{timestamp}.log`
- `delete_old_seed_data_{env}_{timestamp}.log`

## Error Handling

- Scripts use comprehensive error handling
- Failed operations are logged with detailed error messages
- Dry-run mode allows safe testing before execution
- Migration verification catches common issues
- Subfolder-level error reporting

## Backup and Recovery

- Deletion script creates manifest files before deletion
- Original data remains in source bucket until explicitly deleted
- Parallel operation during transition periods for safety
- Migration verification ensures data integrity

## Data Characteristics

- **Pattern:** Warm Isolated Data (Frequently Accessed Reference Data)
- **Write Access:** Manual uploads and automated seed processes
- **Read Access:** Frequent access by seed data import job
- **File Types:** CSV, Parquet, Avro files across 50+ subfolders
- **Risk Level:** Medium (single consumer but frequently accessed)
- **Cost Impact:** Good (better organization, dedicated lifecycle policies)

## Migration Benefits

- **Excellent Isolation:** 50+ subfolders with complete horizontal isolation
- **Clear Structure:** Single consumer pipeline with well-defined patterns
- **High Volume Impact:** Largest isolated use case for reference data
- **Better Organization:** Dedicated bucket for all reference data
- **Lifecycle Optimization:** Tailored storage policies for reference data access patterns

## Testing Strategy

### Pre-Migration Testing
1. Run dry-run to analyze scope
2. Verify all 50+ subfolders are detected
3. Check file type distribution
4. Estimate migration time and costs

### Post-Migration Testing
1. Verify file counts match between old and new locations
2. Test sample file access from new bucket
3. Run seed import job end-to-end
4. Verify BigQuery seed dataset is populated
5. Check downstream applications using seed data

## Performance Considerations

- **Large Dataset:** 50+ subfolders with mixed file formats
- **Warm Access:** Frequently accessed, so minimize downtime
- **Parallel Processing:** rsync handles recursive copying efficiently
- **Network Transfer:** Plan for data transfer time and costs
- **Incremental Approach:** Consider migrating subfolders in batches if needed

## Rollback Plan

If migration issues occur:
1. Code references can be quickly reverted
2. Old bucket data remains available during transition
3. Applications can fall back to old paths immediately
4. No data loss risk during migration process

## Notes

- This is the largest single migration in terms of file diversity
- Warm access pattern requires coordination with data consumers
- Single consumer makes rollback simpler
- Consider implementing retention policies after successful migration
- Monitor performance impact on seed data import jobs
