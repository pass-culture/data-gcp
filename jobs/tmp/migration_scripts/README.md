# Bucket Migration Scripts

A comprehensive, production-ready solution for migrating files between Google Cloud Storage buckets with support for different migration patterns and use cases.

## Overview

This migration framework provides a configuration-driven approach to handle various bucket migration scenarios:

- **Single Folder**: Migrate entire folder contents
- **Multi Folder**: Migrate multiple folders with optional subfolder mapping
- **File Pattern**: Migrate files matching specific patterns with regex support
- **Date Partitioned**: Migrate date-partitioned data with time range filtering

## Architecture

```
migration_scripts/
├── migrate_bucket_data.py      # Main orchestrator script
├── config/                     # Migration configurations
│   ├── recommendation_sync.yaml
│   ├── seed_data.yaml
│   ├── clickhouse_export.yaml
│   ├── dms_export.yaml
│   └── historization.yaml
├── patterns/                   # Migration pattern implementations
│   ├── base_pattern.py        # Abstract base class
│   ├── single_folder.py       # Single folder migration
│   ├── multi_folder.py        # Multi folder with mapping
│   ├── file_pattern.py        # Pattern-based migration
│   └── date_partitioned.py    # Date-aware migration
└── utils/                      # Utility modules
    ├── gcs_client.py          # GCS operations wrapper
    ├── validation.py          # Pre/post migration validation
    └── logging_utils.py       # Comprehensive logging
```

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure you have Google Cloud credentials configured:
```bash
# Either set environment variable
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Or use gcloud auth
gcloud auth application-default login
```

## Usage

### Quick Start

```bash
# Dry run to analyze what would be migrated
python migrate_bucket_data.py --use-case recommendation_sync --env dev --dry-run

# Run actual migration
python migrate_bucket_data.py --use-case recommendation_sync --env dev

# Use custom configuration file
python migrate_bucket_data.py --config config/custom_migration.yaml --env prod
```

### Command Line Options

- `--config` / `-c`: Path to custom configuration file
- `--use-case` / `-u`: Predefined use case (recommendation_sync, seed_data, clickhouse_export, dms_export, historization)
- `--env` / `-e`: Environment (dev, stg, prod)
- `--dry-run` / `-n`: Analyze without migrating
- `--log-level` / `-l`: Logging level (DEBUG, INFO, WARNING, ERROR)

## Configuration

### Single Pattern Configuration

```yaml
use_case: "example_migration"
pattern: "single_folder"
source_bucket: "source-bucket-{env}"
source_path: "data/folder/"
target_bucket: "target-bucket-{env}"
target_path: "new/location/"
batch_size: 100
validation:
  verify_checksums: true
cleanup_after_migration: false
```

### Multi Pattern Configuration

```yaml
use_case: "complex_migration"
patterns:
  - pattern: "file_pattern"
    name: "json_files"
    file_patterns: ["*.json"]
    # ... other settings
  - pattern: "date_partitioned"
    name: "historical_data"
    days_back: 90
    # ... other settings
```

## Migration Patterns

### Single Folder Pattern
- Migrates entire folder contents
- Supports recursive subfolder inclusion
- File type filtering

### Multi Folder Pattern
- Handles multiple source folders
- Optional subfolder mapping for reorganization
- Ideal for seed data with many categories

### File Pattern Pattern
- Regex-based file matching
- Include/exclude patterns
- File renaming capabilities
- Pattern analysis in dry run

### Date Partitioned Pattern
- Date-aware file discovery
- Time range filtering (start_date, end_date, days_back)
- Date-based target restructuring
- Enhanced retention policies

## Validation

The framework includes comprehensive validation:

### Pre-Migration
- Configuration validation
- Bucket existence verification
- Permission checks

### Post-Migration
- File count verification
- Optional checksum validation
- Optional file size verification

## Logging

Comprehensive logging with:
- File and console output
- Progress tracking with statistics
- Operation-level logging
- Migration summary reports
- Configurable log levels

## Use Cases

### Recommendation Sync
```bash
python migrate_bucket_data.py --use-case recommendation_sync --env dev --dry-run
```
Migrates bidirectional staging files for BigQuery ↔ GCS ↔ CloudSQL pipeline.

### Seed Data
```bash
python migrate_bucket_data.py --use-case seed_data --env prod
```
Migrates 50+ seed data subfolders with organized mapping.

### ClickHouse Export
```bash
python migrate_bucket_data.py --use-case clickhouse_export --env stg
```
Migrates temporary staging files for BigQuery → GCS → ClickHouse pipeline.

### DMS Export
```bash
python migrate_bucket_data.py --use-case dms_export --env prod
```
Migrates complete ETL pipeline: API → JSON → Parquet → BigQuery.

### Historization
```bash
python migrate_bucket_data.py --use-case historization --env prod
```
Migrates multiple archival processes with different retention strategies.

## Safety Features

- **Dry Run Mode**: Analyze before executing
- **Batch Processing**: Prevents memory issues with large datasets
- **Comprehensive Validation**: Pre and post-migration checks
- **Configurable Cleanup**: Optional source file deletion with retention policies
- **Progress Tracking**: Real-time migration progress with statistics
- **Error Handling**: Graceful failure handling with detailed logging

## Best Practices

1. **Always run dry run first** to understand the scope
2. **Test in dev environment** before production migrations
3. **Use appropriate batch sizes** based on file sizes
4. **Enable validation** for critical data migrations
5. **Configure retention policies** carefully for cleanup
6. **Monitor logs** for any issues during migration
7. **Keep source files** until validation is complete

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure service account has necessary GCS permissions
2. **Bucket Not Found**: Verify bucket names and environment variables
3. **Out of Memory**: Reduce batch size for large files
4. **Pattern Not Matching**: Test regex patterns with sample file names
5. **Date Parsing Errors**: Verify date format and partition patterns

### Debug Mode

```bash
python migrate_bucket_data.py --use-case example --env dev --log-level DEBUG --dry-run
```

This will provide detailed logging for troubleshooting.

## Contributing

When adding new migration patterns:

1. Extend `BaseMigrationPattern`
2. Implement `discover_files()` and `migrate_batch()` methods
3. Add pattern class to `MigrationOrchestrator.PATTERN_CLASSES`
4. Create configuration examples
5. Update documentation
