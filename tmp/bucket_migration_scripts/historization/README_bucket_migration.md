# Bucket Migration Scripts

This directory contains scripts for migrating historical data from the old bucket structure to the new dedicated archive buckets, following the migration implemented in commit `d216232ee`.

## Overview

The bucket migration moves data from:
- **Old structure**: `gs://data-bucket-{env}/historization/`
- **New structure**: Dedicated archive buckets:
  - `gs://de-bigquery-data-archive-{env}/` (DE team data)
  - `gs://ds-data-archive-{env}/` (DS team data)

## Scripts

### 1. `migrate_historical_data.py`

Migrates historical data from the old `/historization` folder to the new archive buckets.

#### Usage

```bash
# Dry run (safe - shows what would be migrated)
python scripts/migrate_historical_data.py --env prod --days 30 --dry-run

# Execute migration (requires confirmation)
python scripts/migrate_historical_data.py --env prod --days 30 --execute
```

#### Migration Paths

| Old Path | New Path | Description |
|----------|----------|-------------|
| `historization/tracking/` | `de-bigquery-data-archive-{env}/historization/tracking/` | Firebase tracking events |
| `historization/int_firebase/` | `de-bigquery-data-archive-{env}/historization/int_firebase/` | Firebase intermediate events |
| `historization/api_reco/` | `ds-data-archive-{env}/historization/api_reco/` | API recommendation data |
| `historization/applicative/` | `de-bigquery-data-archive-{env}/historization/applicative/` | Applicative database snapshots |

#### Features

- **Date-based filtering**: Migrates only files from the last N days
- **Dry-run mode**: Default mode that shows what would be migrated
- **Verification**: Checks that target buckets exist and data was transferred
- **Comprehensive logging**: Creates detailed log files with timestamps
- **Progress tracking**: Shows migration progress for each data type

### 2. `delete_historization_folder.py`

Safely deletes the old `/historization` folder after successful migration.

#### Usage

```bash
# Dry run (safe - shows what would be deleted)
python scripts/delete_historization_folder.py --env prod --dry-run

# Execute deletion (requires multiple confirmations)
python scripts/delete_historization_folder.py --env prod --confirm-delete
```

#### Safety Features

- **Multiple confirmations**: Requires explicit confirmation text
- **Backup manifest**: Creates JSON file with all deleted file metadata
- **Pre-deletion checks**: Verifies new archive buckets exist
- **File inventory**: Lists all files to be deleted with sizes
- **Comprehensive logging**: Detailed logs of all operations

⚠️ **WARNING**: This script permanently deletes data. Use with extreme caution!

## Migration Process

### Step 1: Plan and Verify

1. Review the files modified in commit `d216232ee` to understand the changes
2. Verify new archive buckets exist and are properly configured
3. Test migration scripts in development environment first

### Step 2: Execute Migration

```bash
# 1. Start with a dry run to see what will be migrated
python scripts/migrate_historical_data.py --env prod --days 30 --dry-run

# 2. Execute the actual migration
python scripts/migrate_historical_data.py --env prod --days 30 --execute

# 3. Verify migration was successful by checking new buckets
gcloud storage ls --recursive gs://de-bigquery-data-archive-prod/
gcloud storage ls --recursive gs://ds-data-archive-prod/
```

### Step 3: Cleanup (Optional)

Only after verifying successful migration and that systems are working correctly:

```bash
# 1. Dry run to see what would be deleted
python scripts/delete_historization_folder.py --env prod --dry-run

# 2. Execute deletion (creates backup manifest first)
python scripts/delete_historization_folder.py --env prod --confirm-delete
```

## Environment Support

Both scripts support all environments:
- `dev`: Development environment
- `stg`: Staging environment
- `prod`: Production environment

## Logging and Monitoring

Both scripts create detailed log files:
- Migration logs: `migration_{env}_{timestamp}.log`
- Deletion logs: `delete_historization_{env}_{timestamp}.log`
- Backup manifests: `historization_manifest_{env}_{timestamp}.json`

## Error Handling

The scripts include comprehensive error handling:
- Bucket existence verification
- File transfer verification
- Proper subprocess error handling
- Detailed error messages and logging

## Testing

Always test in development environment first:

```bash
# Test migration
python scripts/migrate_historical_data.py --env dev --days 7 --dry-run
python scripts/migrate_historical_data.py --env dev --days 7 --execute

# Test deletion (if appropriate)
python scripts/delete_historization_folder.py --env dev --dry-run
```

## Rollback Plan

If issues are discovered after migration:

1. **Migration rollback**: Use `gcloud storage rsync` to copy data back from new buckets to old structure
2. **Deletion rollback**: Use the backup manifest JSON file to identify deleted files (files cannot be recovered after deletion)

## Related Files

- `jobs/etl_jobs/internal/bigquery_archive_partition/archive.py` - Updated archive logic
- `orchestration/dags/jobs/administration/bigquery_archive_partition.py` - Archive DAG configuration
- `orchestration/dags/common/config.py` - Bucket configuration

## Support

For issues with these scripts:
1. Check the log files for detailed error information
2. Verify bucket permissions and existence
3. Ensure `gcloud` CLI is properly configured and authenticated
4. Review the commit changes to understand the migration context
