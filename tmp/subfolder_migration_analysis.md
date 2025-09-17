# Subfolder-Focused Bucket Migration Analysis

## Overview

This analysis takes a **subfolder-first approach** to identify horizontal use cases for progressive bucket migration. Each subfolder is analyzed for its usage patterns, isolation level, and migration complexity to develop a practical migration roadmap.

**Goal**: Migrate progressively identified use cases to dedicated buckets to optimize infrastructure and reduce costs.

---

## Subfolder Analysis (Sorted by Migration Priority)

## 🔴 HIGH PRIORITY SUBFOLDERS

### **bigquery_imports/seed** 🔴 HIGH PRIORITY
**Pattern**: Reference Data Repository (50+ subfolders)
- **WRITE**: Manual uploads and automated seed processes
- **READ**: `jobs/etl_jobs/internal/gcs_seed/main.py`
  - **50+ Reference Tables** including:
    - Geographic data: `iris_france`, `region_department`, `deps_qpv_2017`
    - Institution data: `institution_metadata`, `macro_rayons`
    - Business data: `siren_main_business_labels`, `eac_cash_in`
    - Compliance data: Various `deps_*` tables
  - Format: CSV, Parquet, Avro files
  - Destination: `seed_{ENV_SHORT_NAME}` BigQuery dataset
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - Largest isolated use case (50+ subfolders)
  - Complete horizontal isolation
  - Single consumer pipeline
  - Clear reference data pattern

---

### **dms_export** 🔴 HIGH PRIORITY
**Pattern**: ETL Staging Pipeline (API → JSON → Parquet → BigQuery)
- **WRITE**: Multiple operations in sequence:
  1. `jobs/etl_jobs/external/dms/main.py` writes JSON: `dms_export/unsorted_dms_{target}_{updated_since}.json`
  2. `jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py` converts to Parquet: `dms_export/dms_{target}_{updated_since}.parquet`
- **READ**: `orchestration/dags/jobs/import/import_dms_subscriptions.py`
  - Reads: `dms_export/dms_jeunes_{updated_since}.parquet`
  - Reads: `dms_export/dms_pro_{updated_since}.parquet`
  - Imports to BigQuery for further processing
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - Complete ETL staging pipeline with clear data transformation flow
  - Horizontal isolation (single use case: DMS processing)
  - Multiple file formats (JSON→Parquet) within same subfolder
  - Well-defined consumer pattern

---

### **QPI_exports** 🔴 HIGH PRIORITY
**Pattern**: External Import Pipeline (External writes → Internal processing)
- **WRITE**: External QPI system writes daily exports
  - Path: `QPI_exports/qpi_answers_{YYYYMMDD}/`
  - Format: `*.jsonl` files
- **READ**: `orchestration/dags/jobs/import/import_qpi.py`
  - Checks: `QPI_exports/qpi_answers_{today}/` for file presence
  - Imports: `QPI_exports/qpi_answers_{ds_nodash}/*.jsonl` → BigQuery
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - Complete horizontal isolation
  - Clear external→internal pipeline
  - Single consumer pipeline
  - No dependencies on other subfolders

---

### **export/cloudsql_recommendation_tables_to_bigquery** 🔴 HIGH PRIORITY
**Pattern**: Bidirectional Sync Staging (BigQuery ↔ GCS ↔ CloudSQL)
- **WRITE**: `orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py`
  - Exports BigQuery data → GCS: `export/cloudsql_recommendation_tables_to_bigquery/{timestamp}/`
  - Uses: `jobs/etl_jobs/internal/sync_recommendation/jobs/daily_bq_to_sql/export_to_gcs.py`
- **READ**: Two consumers:
  1. Same DAG reads GCS data → imports to CloudSQL via `gcs_to_sql.py`
  2. `orchestration/dags/jobs/api/sync_cloudsql_recommendation_tables_to_bigquery.py` for reverse sync
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - Complete bidirectional staging pipeline
  - Horizontal isolation (recommendation sync only)
  - Temporary staging with clear lifecycle
  - Multiple coordinated operations within single use case

---

### **historization/** 🔴 HIGH PRIORITY
**Pattern**: BigQuery Archival System (Multiple organized subfolders)
- **WRITE**: Two archival processes:
  1. `orchestration/dags/jobs/administration/bigquery_archive_partition.py` → `historization/{folder}/{table_id}/{partition}/`
     - Subfolders: `tracking/`, `int_firebase/`, `api_reco/`
  2. `orchestration/dags/jobs/administration/bigquery_historize_applicative_database.py` → `historization/applicative/{table}/{YYYYMMDD}/`
- **READ**: Currently minimal, mainly for data recovery/analysis
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - Complete archival system with organized subfolders by data type
  - Clear write-heavy pattern (archival storage)
  - Well-structured subfolder organization
  - Self-contained archival operations

---

### **archives** 🔴 HIGH PRIORITY
**Pattern**: Manual BigQuery Table Archives
- **WRITE**: Manual BigQuery table exports (inferred)
- **READ**: Manual/adhoc analysis (inferred)
- **Migration Assessment**: 🔄 **NEEDS INVESTIGATION**
  - Unclear usage patterns
  - Potentially large cold storage
  - May be candidates for deletion or BigQuery archiving

---

## 🟡 MEDIUM PRIORITY SUBFOLDERS

### **clickhouse_export** 🟡 MEDIUM PRIORITY
**Pattern**: Temporary Staging (Write → Read → Export pipeline)
- **WRITE**: `orchestration/dags/jobs/export/export_clickhouse.py`
  - Path: `{DATA_GCS_BUCKET_NAME}/clickhouse_export/{ENV_SHORT_NAME}/export/{DATE}`
  - Exports BigQuery data to GCS as intermediate staging
- **READ**: Same DAG reads the staged GCS data and forwards to ClickHouse
- **Migration Assessment**: ✅ **GOOD CANDIDATE**
  - Single-purpose export pipeline with temporary staging
  - Complete horizontal isolation (single DAG manages both write/read)
  - Clear staging pattern: BigQuery → GCS → ClickHouse

---

### **elementary_reports** 🟡 MEDIUM PRIORITY
**Pattern**: Report Storage (Write data quality reports)
- **WRITE**: `orchestration/dags/jobs/dbt/dbt_artifacts.py`
  - Path: `elementary_reports/{year}/elementary_report_{YYYYMMDD}.html`
  - Creates data quality monitoring reports
- **READ**: Manual access for report viewing (inferred)
- **Migration Assessment**: 🔄 **CONSIDER RETENTION POLICY**
  - Growing storage (daily reports)
  - May need archival/pruning strategy
  - Single writer, manual readers

---

### **QPI_historical** 🟡 MEDIUM PRIORITY
**Pattern**: Cold Storage (Read-only historical data)
- **WRITE**: None (historical data only)
- **READ**: `orchestration/dags/jobs/import/import_qpi.py`
  - Path: `QPI_historical/qpi_answers_historical_*.parquet`
  - Destination: BigQuery raw dataset
- **Migration Assessment**: 🔄 **SHOULD MIGRATE TO SEED**
  - Cold storage data, used for historical context
  - Better suited for `bigquery_imports/seed/` as reference data
  - Single consumer, minimal impact

---

### **qpi_mapping** 🟡 MEDIUM PRIORITY
**Pattern**: Reference Data (Read-only mapping table)
- **WRITE**: Manual uploads (infrequent)
- **READ**: `jobs/etl_jobs/internal/gcs_seed/main.py`
  - Part of `REF_TABLES` configuration
  - Path: `bigquery_imports/seed/qpi_mapping/`
  - Format: Parquet files
- **Migration Assessment**: ✅ **ALREADY IN SEED STRUCTURE**
  - Part of existing seed data pattern
  - No additional migration needed

---

### **bigquery_archive_tables** 🟡 MEDIUM PRIORITY
**Pattern**: Legacy Archive System
- **WRITE**: Legacy archiving processes (potentially deprecated)
- **READ**: Unknown/minimal (inferred)
- **Migration Assessment**: 🗑️ **CANDIDATE FOR CLEANUP**
  - Legacy system, potentially unused
  - Should validate if still needed
  - May be safe to delete

---

### **dump_wiki_data** 🟡 MEDIUM PRIORITY
**Pattern**: Monthly Wikidata Extractions
- **WRITE**: `orchestration/dags/jobs/ml/artist_wikidata_dump.py`
  - Path: `gs://{DATA_GCS_BUCKET_NAME}/dump_wikidata/{YYYYMMDD}/`
  - File: `wikidata_extraction.parquet`
  - Schedule: Monthly (CRON: `0 3 1 * *`)
- **READ**: Artist linkage ML processes (inferred)
- **Migration Assessment**: 🤔 **CONSIDER ML BUCKET**
  - ML-related data, could use existing `ML_BUCKET_TEMP`
  - Scheduled generation, clear pattern
  - Medium isolation (ML-specific use case)

---

### **artists** (link_artists) 🟡 MEDIUM PRIORITY
**Pattern**: ML Artist Linkage Data
- **WRITE**: ML processing pipelines (inferred)
- **READ**: Multiple ML Streamlit applications
  - `jobs/ml_jobs/artist_linkage/streamlits/st_explore_and_clusterize_data.py`
  - `jobs/ml_jobs/artist_linkage/streamlits/st_analyze_clustering.py`
  - Reads: `gs://data-bucket-stg/link_artists/artists_to_match.parquet`
  - Reads: `gs://data-bucket-stg/link_artists/matched_artists.parquet`
- **Migration Assessment**: 🤔 **USE EXISTING ML_BUCKET_TEMP**
  - ML-specific use case
  - Already have `data-bucket-ml-temp-{ENV_SHORT_NAME}`
  - Multiple ML consumers

---

## 🟢 LOW PRIORITY SUBFOLDERS

### **base32-encode** 🟢 LOW PRIORITY
**Pattern**: Utility Library (Read-only JavaScript resource)
- **WRITE**: Manual deployment (rare updates)
- **READ**: `orchestration/dags/common/config.py`
  - Path: `gs://data-bucket-{env}/base32-encode/base32.js`
  - Used by: BigQuery UDF JavaScript functions
  - Files: `orchestration/dags/data_gcp_dbt/macros/function_utils/udf_js/*.sql`
- **Migration Assessment**: 🤔 **KEEP IN MAIN BUCKET**
  - Small utility, minimal storage impact
  - Shared across multiple BigQuery UDFs
  - Low complexity, low benefit to migrate

---

## Migration Priority Ranking

### **Tier 1: Immediate Migration (High Impact, Low Risk)**
1. **bigquery_imports/seed** - Largest isolated use case (50+ subfolders)
2. **dms_export** - Complete ETL staging pipeline with JSON→Parquet transformation
3. **QPI_exports** - Perfect horizontal isolation, external→internal pipeline
4. **export/cloudsql_recommendation_tables_to_bigquery** - Complete bidirectional sync staging
5. **historization/** - Complete archival system with organized subfolders

### **Tier 2: Medium-term Migration (Good Isolation, Moderate Impact)**
6. **clickhouse_export** - Temporary staging pipeline (BigQuery→GCS→ClickHouse)
7. **elementary_reports** - Consider retention policy first
8. **archives** - Investigate usage patterns first

### **Tier 3: Evaluate & Optimize (Special Handling)**
7. **QPI_historical** - Migrate to seed structure
8. **dump_wiki_data** - Consider ML bucket consolidation
9. **artists** - Use existing ML bucket infrastructure

### **Tier 4: Clean-up & Consolidate (Low Priority)**
10. **bigquery_archive_tables** - Validate if needed, potential cleanup
11. **base32-encode** - Keep in main bucket (utility, low impact)

---

## Progressive Migration Roadmap

### **Phase 1: Staging Pipelines (Month 1)**
- **dms_export** → `dms-staging-bucket-{ENV_SHORT_NAME}`
  - Complete ETL staging pipeline, clear transformation flow
  - High isolation, well-defined lifecycle
- **export/cloudsql_recommendation_tables_to_bigquery** → `recommendation-sync-bucket-{ENV_SHORT_NAME}`
  - Bidirectional sync staging, temporary data lifecycle

### **Phase 2: Reference & External Data (Month 2)**
- **bigquery_imports/seed** → `seed-data-bucket-{ENV_SHORT_NAME}`
  - 50+ reference tables, largest use case
- **QPI_exports** → `qpi-import-bucket-{ENV_SHORT_NAME}`
  - External system dependency, clear isolation
- **clickhouse_export** → `clickhouse-staging-bucket-{ENV_SHORT_NAME}`
  - Temporary staging for external system

### **Phase 3: Archival Systems (Month 3)**
- **historization/** → `archive-bucket-{ENV_SHORT_NAME}`
  - Complete archival system migration
- Investigate **archives** subfolder usage

### **Phase 4: Optimization & Cleanup (Month 4)**
- **QPI_historical** → Migrate to seed bucket
- **dump_wiki_data** + **artists** → Consolidate with ML bucket
- Cleanup **bigquery_archive_tables** if unused
- Implement **elementary_reports** retention policy

---

## Implementation Strategy

### **Bucket Naming Convention**
```
{use-case}-bucket-{env}
Examples:
- seed-data-bucket-dev
- qpi-import-bucket-prod
- archive-bucket-stg
```

### **Migration Steps per Subfolder**
1. **Create dedicated bucket** with appropriate lifecycle policies
2. **Update code references** to new bucket paths
3. **Migrate existing data** (if needed)
4. **Test end-to-end workflows**
5. **Monitor & validate** post-migration
6. **Clean up old subfolder** after validation period

### **Risk Mitigation**
- Start with **read-only** subfolders first (lower risk)
- Maintain **parallel operations** during transition periods
- Implement **monitoring** for bucket usage and access patterns
- Plan **rollback procedures** for each migration phase

---

## Expected Benefits

### **Cost Optimization**
- Dedicated lifecycle policies per use case
- Cold storage for archival data
- Regional optimization for specific workflows

### **Security & Access Control**
- Granular bucket-level permissions
- Isolated blast radius for access issues
- Better compliance for sensitive data

### **Operational Excellence**
- Clear ownership boundaries
- Simplified troubleshooting
- Better monitoring and alerting per use case

### **Infrastructure Scalability**
- Use case-specific optimization
- Better resource allocation
- Foundation for future growth

---

## Migration Script Architecture

### **Overview**
A flexible Python script architecture to handle different subfolder migration patterns with minimal code duplication. The script supports various migration scenarios through a configuration-driven approach.

### **Script Structure**

```
migration_scripts/
├── migrate_bucket_data.py          # Main migration orchestrator
├── config/
│   ├── migration_configs.yaml     # Migration configurations per use case
│   └── bucket_mappings.yaml       # Source → Target bucket mappings
├── patterns/
│   ├── __init__.py
│   ├── base_pattern.py            # Base migration pattern class
│   ├── single_folder.py           # Single folder migration
│   ├── multi_folder.py            # Multiple folders migration
│   ├── file_pattern.py            # File name pattern migration
│   └── date_partitioned.py        # Date-partitioned data migration
└── utils/
    ├── __init__.py
    ├── gcs_client.py              # GCS operations wrapper
    ├── validation.py              # Pre/post migration validation
    └── logging_utils.py           # Migration logging utilities
```

### **Configuration-Driven Approach**

#### **migration_configs.yaml**
```yaml
# High Priority Use Cases
bigquery_imports_seed:
  pattern: "multi_folder"
  source_bucket: "data-bucket-{env}"
  target_bucket: "seed-data-bucket-{env}"
  source_path: "bigquery_imports/seed/"
  target_path: ""  # Root of target bucket
  include_subfolders: true
  file_types: ["*.csv", "*.parquet", "*.avro"]
  validation:
    check_file_count: true
    verify_checksums: true
  lifecycle_policy: "standard_with_archiving"

dms_export:
  pattern: "file_pattern"
  source_bucket: "data-bucket-{env}"
  target_bucket: "dms-staging-bucket-{env}"
  source_path: "dms_export/"
  target_path: "staging/"
  file_patterns:
    - "dms_*.parquet"
    - "unsorted_dms_*.json"
  retention_days: 90
  validation:
    check_file_count: true
    verify_file_sizes: true

qpi_exports:
  pattern: "date_partitioned"
  source_bucket: "data-bucket-{env}"
  target_bucket: "qpi-import-bucket-{env}"
  source_path: "QPI_exports/"
  target_path: "imports/"
  partition_pattern: "qpi_answers_{YYYYMMDD}/"
  date_range: "last_30_days"  # or "all", "YYYY-MM-DD:YYYY-MM-DD"
  file_types: ["*.jsonl"]

recommendation_sync:
  pattern: "single_folder"
  source_bucket: "data-bucket-{env}"
  target_bucket: "recommendation-sync-bucket-{env}"
  source_path: "export/cloudsql_recommendation_tables_to_bigquery/"
  target_path: "sync_staging/"
  include_subfolders: true
  cleanup_after_migration: true  # Remove from source after successful migration

historization:
  pattern: "multi_folder"
  source_bucket: "data-bucket-{env}"
  target_bucket: "archive-bucket-{env}"
  source_path: "historization/"
  target_path: "archives/"
  subfolder_mapping:
    "tracking/": "firebase_tracking/"
    "int_firebase/": "firebase_events/"
    "api_reco/": "recommendation/"
    "applicative/": "database_snapshots/"
  lifecycle_policy: "immediate_coldline"

clickhouse_export:
  pattern: "date_partitioned"
  source_bucket: "data-bucket-{env}"
  target_bucket: "clickhouse-staging-bucket-{env}"
  source_path: "clickhouse_export/"
  target_path: "staging/"
  partition_pattern: "{env}/export/{YYYYMMDD}/"
  cleanup_after_migration: true
  retention_days: 7  # Short retention for staging data
```

### **Migration Pattern Classes**

#### **base_pattern.py**
```python
from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import logging
from utils.gcs_client import GCSClient
from utils.validation import ValidationUtils

class BaseMigrationPattern(ABC):
    """Base class for all migration patterns"""

    def __init__(self, config: Dict, env: str):
        self.config = config
        self.env = env
        self.gcs_client = GCSClient()
        self.validator = ValidationUtils()
        self.logger = logging.getLogger(f"migration.{self.__class__.__name__}")

    @abstractmethod
    def discover_files(self) -> List[Dict]:
        """Discover files to migrate based on pattern"""
        pass

    @abstractmethod
    def migrate_batch(self, files: List[Dict]) -> bool:
        """Migrate a batch of files"""
        pass

    def pre_migration_validation(self) -> bool:
        """Validate before starting migration"""
        return self.validator.validate_buckets_exist(
            self.config['source_bucket'].format(env=self.env),
            self.config['target_bucket'].format(env=self.env)
        )

    def post_migration_validation(self, migrated_files: List[Dict]) -> bool:
        """Validate after migration completion"""
        if self.config.get('validation', {}).get('check_file_count'):
            return self.validator.verify_file_counts(migrated_files)
        return True

    def execute(self) -> bool:
        """Main execution method"""
        self.logger.info(f"Starting migration for pattern: {self.__class__.__name__}")

        if not self.pre_migration_validation():
            self.logger.error("Pre-migration validation failed")
            return False

        files_to_migrate = self.discover_files()
        self.logger.info(f"Discovered {len(files_to_migrate)} files to migrate")

        # Migrate in batches
        batch_size = self.config.get('batch_size', 100)
        migrated_files = []

        for i in range(0, len(files_to_migrate), batch_size):
            batch = files_to_migrate[i:i+batch_size]
            if self.migrate_batch(batch):
                migrated_files.extend(batch)
            else:
                self.logger.error(f"Failed to migrate batch {i//batch_size + 1}")
                return False

        if not self.post_migration_validation(migrated_files):
            self.logger.error("Post-migration validation failed")
            return False

        self.logger.info(f"Successfully migrated {len(migrated_files)} files")
        return True
```

#### **single_folder.py**
```python
from patterns.base_pattern import BaseMigrationPattern
from typing import Dict, List

class SingleFolderPattern(BaseMigrationPattern):
    """Migration pattern for entire folder contents"""

    def discover_files(self) -> List[Dict]:
        """Discover all files in source folder"""
        source_bucket = self.config['source_bucket'].format(env=self.env)
        source_path = self.config['source_path']
        include_subfolders = self.config.get('include_subfolders', False)

        return self.gcs_client.list_files(
            bucket=source_bucket,
            prefix=source_path,
            recursive=include_subfolders,
            file_types=self.config.get('file_types', ['*'])
        )

    def migrate_batch(self, files: List[Dict]) -> bool:
        """Migrate a batch of files"""
        source_bucket = self.config['source_bucket'].format(env=self.env)
        target_bucket = self.config['target_bucket'].format(env=self.env)
        source_path = self.config['source_path']
        target_path = self.config['target_path']

        for file_info in files:
            # Calculate target path by replacing source prefix with target prefix
            relative_path = file_info['name'].replace(source_path, '', 1)
            target_file_path = f"{target_path}{relative_path}".strip('/')

            success = self.gcs_client.copy_file(
                source_bucket=source_bucket,
                source_path=file_info['name'],
                target_bucket=target_bucket,
                target_path=target_file_path
            )

            if not success:
                self.logger.error(f"Failed to copy {file_info['name']}")
                return False

            # Optional: Delete from source after successful copy
            if self.config.get('cleanup_after_migration', False):
                self.gcs_client.delete_file(source_bucket, file_info['name'])

        return True
```

#### **file_pattern.py**
```python
from patterns.base_pattern import BaseMigrationPattern
from typing import Dict, List
import fnmatch

class FilePatternMigration(BaseMigrationPattern):
    """Migration pattern for specific file name patterns"""

    def discover_files(self) -> List[Dict]:
        """Discover files matching specified patterns"""
        source_bucket = self.config['source_bucket'].format(env=self.env)
        source_path = self.config['source_path']
        file_patterns = self.config.get('file_patterns', ['*'])

        all_files = self.gcs_client.list_files(
            bucket=source_bucket,
            prefix=source_path,
            recursive=False
        )

        # Filter files by patterns
        matching_files = []
        for file_info in all_files:
            file_name = file_info['name'].split('/')[-1]  # Get filename only

            for pattern in file_patterns:
                if fnmatch.fnmatch(file_name, pattern):
                    matching_files.append(file_info)
                    break

        return matching_files

    def migrate_batch(self, files: List[Dict]) -> bool:
        """Migrate files maintaining folder structure"""
        source_bucket = self.config['source_bucket'].format(env=self.env)
        target_bucket = self.config['target_bucket'].format(env=self.env)
        source_path = self.config['source_path']
        target_path = self.config['target_path']

        for file_info in files:
            # Preserve subfolder structure in target
            relative_path = file_info['name'].replace(source_path, '', 1)
            target_file_path = f"{target_path}{relative_path}".strip('/')

            success = self.gcs_client.copy_file(
                source_bucket=source_bucket,
                source_path=file_info['name'],
                target_bucket=target_bucket,
                target_path=target_file_path
            )

            if not success:
                return False

        return True
```

#### **date_partitioned.py**
```python
from patterns.base_pattern import BaseMigrationPattern
from typing import Dict, List
from datetime import datetime, timedelta
import re

class DatePartitionedPattern(BaseMigrationPattern):
    """Migration pattern for date-partitioned data"""

    def discover_files(self) -> List[Dict]:
        """Discover files in date-partitioned folders"""
        source_bucket = self.config['source_bucket'].format(env=self.env)
        source_path = self.config['source_path']
        partition_pattern = self.config['partition_pattern']
        date_range = self.config.get('date_range', 'all')

        # Generate date range
        dates_to_process = self._generate_date_range(date_range)

        all_files = []
        for date_str in dates_to_process:
            # Replace date placeholders in partition pattern
            folder_pattern = partition_pattern.replace('{YYYYMMDD}', date_str)
            folder_pattern = folder_pattern.replace('{env}', self.env)

            folder_path = f"{source_path}{folder_pattern}"

            files = self.gcs_client.list_files(
                bucket=source_bucket,
                prefix=folder_path,
                file_types=self.config.get('file_types', ['*'])
            )

            # Add date context to file info
            for file_info in files:
                file_info['partition_date'] = date_str

            all_files.extend(files)

        return all_files

    def _generate_date_range(self, date_range: str) -> List[str]:
        """Generate list of date strings based on range specification"""
        if date_range == 'all':
            # Discovery-based approach - scan existing folders
            return self._discover_existing_dates()
        elif date_range == 'last_30_days':
            return self._generate_recent_dates(30)
        elif ':' in date_range:
            # Range format: YYYY-MM-DD:YYYY-MM-DD
            start_date, end_date = date_range.split(':')
            return self._generate_date_range_between(start_date, end_date)
        else:
            return [date_range]  # Single date

    def _discover_existing_dates(self) -> List[str]:
        """Discover existing date partitions in source bucket"""
        source_bucket = self.config['source_bucket'].format(env=self.env)
        source_path = self.config['source_path']

        # List all folders and extract date patterns
        folders = self.gcs_client.list_folders(
            bucket=source_bucket,
            prefix=source_path
        )

        # Extract dates using regex based on partition pattern
        date_pattern = self.config['partition_pattern']
        regex_pattern = date_pattern.replace('{YYYYMMDD}', r'(\d{8})')
        regex_pattern = regex_pattern.replace('{env}', self.env)

        dates = []
        for folder in folders:
            match = re.search(regex_pattern, folder)
            if match:
                dates.append(match.group(1))

        return sorted(list(set(dates)))  # Remove duplicates and sort

    def migrate_batch(self, files: List[Dict]) -> bool:
        """Migrate date-partitioned files"""
        source_bucket = self.config['source_bucket'].format(env=self.env)
        target_bucket = self.config['target_bucket'].format(env=self.env)
        source_path = self.config['source_path']
        target_path = self.config['target_path']

        for file_info in files:
            # Maintain date partition structure in target
            relative_path = file_info['name'].replace(source_path, '', 1)
            target_file_path = f"{target_path}{relative_path}".strip('/')

            success = self.gcs_client.copy_file(
                source_bucket=source_bucket,
                source_path=file_info['name'],
                target_bucket=target_bucket,
                target_path=target_file_path
            )

            if not success:
                return False

            # Optional cleanup with retention policy
            if self.config.get('cleanup_after_migration', False):
                # Check if file is older than retention period
                retention_days = self.config.get('retention_days', 30)
                file_date = datetime.strptime(file_info['partition_date'], '%Y%m%d')

                if datetime.now() - file_date > timedelta(days=retention_days):
                    self.gcs_client.delete_file(source_bucket, file_info['name'])

        return True
```

### **Main Migration Orchestrator**

#### **migrate_bucket_data.py**
```python
#!/usr/bin/env python3
"""
Bucket Data Migration Orchestrator

Usage:
    python migrate_bucket_data.py --env dev --use-case dms_export --dry-run
    python migrate_bucket_data.py --env prod --use-case all --parallel 4
    python migrate_bucket_data.py --config custom_config.yaml --env stg
"""

import argparse
import yaml
import logging
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from patterns.single_folder import SingleFolderPattern
from patterns.multi_folder import MultiFolderPattern
from patterns.file_pattern import FilePatternMigration
from patterns.date_partitioned import DatePartitionedPattern

# Pattern mapping
PATTERN_CLASSES = {
    'single_folder': SingleFolderPattern,
    'multi_folder': MultiFolderPattern,
    'file_pattern': FilePatternMigration,
    'date_partitioned': DatePartitionedPattern,
}

class MigrationOrchestrator:
    def __init__(self, config_path: str, env: str):
        self.env = env
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()

    def _load_config(self, config_path: str) -> Dict:
        """Load migration configuration"""
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)

    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'migration_{self.env}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger('MigrationOrchestrator')

    def migrate_use_case(self, use_case: str, dry_run: bool = False) -> bool:
        """Migrate a specific use case"""
        if use_case not in self.config:
            self.logger.error(f"Use case '{use_case}' not found in configuration")
            return False

        use_case_config = self.config[use_case]
        pattern_name = use_case_config['pattern']

        if pattern_name not in PATTERN_CLASSES:
            self.logger.error(f"Pattern '{pattern_name}' not implemented")
            return False

        self.logger.info(f"Starting migration for use case: {use_case}")

        if dry_run:
            self.logger.info("DRY RUN MODE - No actual file operations will be performed")
            return self._dry_run_migration(use_case, use_case_config, pattern_name)
        else:
            pattern_class = PATTERN_CLASSES[pattern_name]
            migration_pattern = pattern_class(use_case_config, self.env)
            return migration_pattern.execute()

    def migrate_all(self, parallel: int = 1, dry_run: bool = False) -> Dict[str, bool]:
        """Migrate all configured use cases"""
        results = {}

        if parallel == 1:
            # Sequential execution
            for use_case in self.config:
                results[use_case] = self.migrate_use_case(use_case, dry_run)
        else:
            # Parallel execution
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                future_to_use_case = {
                    executor.submit(self.migrate_use_case, use_case, dry_run): use_case
                    for use_case in self.config
                }

                for future in as_completed(future_to_use_case):
                    use_case = future_to_use_case[future]
                    try:
                        results[use_case] = future.result()
                    except Exception as exc:
                        self.logger.error(f"Use case {use_case} generated an exception: {exc}")
                        results[use_case] = False

        return results

    def _dry_run_migration(self, use_case: str, config: Dict, pattern_name: str) -> bool:
        """Perform dry run to show what would be migrated"""
        self.logger.info(f"DRY RUN: {use_case} using pattern '{pattern_name}'")

        pattern_class = PATTERN_CLASSES[pattern_name]
        migration_pattern = pattern_class(config, self.env)

        # Only discover files, don't migrate
        files_to_migrate = migration_pattern.discover_files()

        self.logger.info(f"DRY RUN: Would migrate {len(files_to_migrate)} files")
        self.logger.info(f"DRY RUN: Source bucket: {config['source_bucket'].format(env=self.env)}")
        self.logger.info(f"DRY RUN: Target bucket: {config['target_bucket'].format(env=self.env)}")

        # Show sample files
        sample_size = min(5, len(files_to_migrate))
        if sample_size > 0:
            self.logger.info("DRY RUN: Sample files to migrate:")
            for file_info in files_to_migrate[:sample_size]:
                self.logger.info(f"  - {file_info['name']} ({file_info.get('size', 'unknown')} bytes)")

        return True

def main():
    parser = argparse.ArgumentParser(description='Migrate bucket data between GCS buckets')
    parser.add_argument('--env', required=True, choices=['dev', 'stg', 'prod'],
                      help='Environment to migrate')
    parser.add_argument('--use-case', default='all',
                      help='Use case to migrate (default: all)')
    parser.add_argument('--config', default='config/migration_configs.yaml',
                      help='Configuration file path')
    parser.add_argument('--dry-run', action='store_true',
                      help='Perform dry run without actual migration')
    parser.add_argument('--parallel', type=int, default=1,
                      help='Number of parallel migrations (only for --use-case all)')

    args = parser.parse_args()

    orchestrator = MigrationOrchestrator(args.config, args.env)

    if args.use_case == 'all':
        results = orchestrator.migrate_all(args.parallel, args.dry_run)

        print(f"\nMigration Results for {args.env}:")
        for use_case, success in results.items():
            status = "✅ SUCCESS" if success else "❌ FAILED"
            print(f"  {use_case}: {status}")

        overall_success = all(results.values())
        exit(0 if overall_success else 1)
    else:
        success = orchestrator.migrate_use_case(args.use_case, args.dry_run)
        exit(0 if success else 1)

if __name__ == "__main__":
    main()
```

### **Usage Examples**

#### **Development Testing**
```bash
# Dry run for single use case
python migrate_bucket_data.py --env dev --use-case dms_export --dry-run

# Migrate single use case
python migrate_bucket_data.py --env dev --use-case bigquery_imports_seed

# Dry run for all use cases
python migrate_bucket_data.py --env dev --use-case all --dry-run
```

#### **Production Migration**
```bash
# Sequential migration (safer)
python migrate_bucket_data.py --env prod --use-case all

# Parallel migration (faster)
python migrate_bucket_data.py --env prod --use-case all --parallel 3

# Migrate specific high-priority use cases only
for use_case in bigquery_imports_seed dms_export qpi_exports; do
    python migrate_bucket_data.py --env prod --use-case $use_case
done
```

### **Key Features**

1. **Configuration-Driven**: Easy to add new use cases without code changes
2. **Multiple Patterns**: Supports different migration scenarios through pluggable patterns
3. **Validation**: Pre and post-migration validation with configurable checks
4. **Dry Run Mode**: Test migrations without moving actual data
5. **Parallel Execution**: Speed up migrations with concurrent processing
6. **Comprehensive Logging**: Full audit trail of migration operations
7. **Error Handling**: Robust error handling with rollback capabilities
8. **Flexible Cleanup**: Configurable source cleanup after successful migration

### **Next Steps**
1. Implement the utility classes (`gcs_client.py`, `validation.py`, etc.)
2. Create environment-specific configuration files
3. Test on development environment with small datasets
4. Implement monitoring and alerting for production migrations
5. Create rollback procedures for failed migrations
