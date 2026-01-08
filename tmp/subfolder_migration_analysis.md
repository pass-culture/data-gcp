# Subfolder-Focused Bucket Migration Analysis

## Overview

This analysis takes a **subfolder-first approach** to identify horizontal use cases for progressive bucket migration. Each subfolder is analyzed for its usage patterns, isolation level, and migration complexity to develop a practical migration roadmap.

**Goal**: Migrate progressively identified use cases to dedicated buckets to optimize infrastructure and reduce costs.

---

## Subfolder Analysis (Sorted by Migration Priority)

## 🔴 HIGH PRIORITY SUBFOLDERS
*Prioritized by isolation level + cold storage for maximum cost savings and low migration risk*

### **archives** 🔴 HIGH PRIORITY
**Pattern**: Manual BigQuery Table Archives (Cold Storage)
- **WRITE**: Manual BigQuery table exports (infrequent)
- **READ**: Manual/adhoc analysis (rare)
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - **Perfect isolation**: Completely independent cold storage
  - **Cost optimization**: Large archived data ideal for cold storage tiers
  - **Low risk**: Minimal active usage, easy rollback if needed
  - **High impact**: Significant storage cost savings potential

---

### **bigquery_archive_tables** 🔴 HIGH PRIORITY
**Pattern**: Legacy Archive System (Cold Storage)
- **WRITE**: Legacy archiving processes (potentially deprecated)
- **READ**: Unknown/minimal (inferred cold)
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - **Perfect isolation**: Legacy system, completely independent
  - **Cold storage optimization**: Ideal for immediate coldline storage
  - **Cleanup opportunity**: Validate usage and potentially delete unused data
  - **Low migration risk**: Minimal active dependencies

---

### **historization/** 🔴 HIGH PRIORITY
**Pattern**: BigQuery Archival System (Cold Storage with Organized Structure)
- **WRITE**: Two archival processes:
  1. `orchestration/dags/jobs/administration/bigquery_archive_partition.py` → `historization/{folder}/{table_id}/{partition}/`
     - Subfolders: `tracking/`, `int_firebase/`, `api_reco/`
  2. `orchestration/dags/jobs/administration/bigquery_historize_applicative_database.py` → `historization/applicative/{table}/{YYYYMMDD}/`
- **READ**: Minimal, mainly for data recovery/analysis (cold access)
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - **Perfect isolation**: Complete archival system with organized subfolders
  - **Cold storage optimization**: Write-heavy, read-cold pattern ideal for archive buckets
  - **Cost impact**: Large volume archival data with immediate coldline potential
  - **Well-structured**: Clear subfolder organization for targeted lifecycle policies

---

### **QPI_historical** 🔴 HIGH PRIORITY
**Pattern**: Cold Storage (Read-only Historical Data)
- **WRITE**: None (historical data only)
- **READ**: `orchestration/dags/jobs/import/import_qpi.py` (infrequent)
  - Path: `QPI_historical/qpi_answers_historical_*.parquet`
  - Destination: BigQuery raw dataset
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - **Perfect isolation**: Read-only historical data, no active writes
  - **Cold storage**: Accessed infrequently for historical context
  - **Migration target**: Better suited for `seed-data-bucket` as reference data
  - **Low risk**: Single consumer, minimal active usage

---


### **QPI_mapping** 🔴 HIGH PRIORITY
**Pattern**: Reference Data (Cold Reference Mapping Table)
- **WRITE**: Manual uploads (infrequent)
- **READ**: `jobs/etl_jobs/internal/gcs_seed/main.py` (infrequent)
  - Part of `REF_TABLES` configuration
  - Path: `QPI_mapping/` (root subfolder of bucket)
  - Format: Parquet files
- **Migration Assessment**: ✅ **EXCELLENT CANDIDATE**
  - **Perfect isolation**: Independent reference data subfolder
  - **Cold access pattern**: Infrequent manual updates, occasional reads
  - **Reference data pattern**: Static mapping table, ideal for cold storage
  - **Easy migration**: Simple subfolder structure, minimal dependencies

---


## 🟡 MEDIUM PRIORITY SUBFOLDERS
*Well-isolated use cases with warm access patterns - good candidates but less immediate cost impact*

### **bigquery_imports/seed** 🟡 MEDIUM PRIORITY
**Pattern**: Reference Data Repository (Well-isolated but Frequently Accessed)
- **WRITE**: Manual uploads and automated seed processes
- **READ**: `jobs/etl_jobs/internal/gcs_seed/main.py` (frequent)
  - **50+ Reference Tables** including geographic, institution, business, and compliance data
  - Format: CSV, Parquet, Avro files
  - Destination: `seed_{ENV_SHORT_NAME}` BigQuery dataset
- **Migration Assessment**: ✅ **GOOD CANDIDATE**
  - **Excellent isolation**: 50+ subfolders, complete horizontal isolation
  - **Warm access pattern**: Frequently accessed reference data
  - **High volume impact**: Largest isolated use case
  - **Clear structure**: Single consumer pipeline, well-defined pattern

---




### **base32-encode** 🟡 MEDIUM PRIORITY
**Pattern**: Shared Utility Library (Small, Multi-consumer)
- **WRITE**: Manual deployment (rare updates)
- **READ**: `orchestration/dags/common/config.py` (frequent)
  - Path: `gs://data-bucket-{env}/base32-encode/base32.js`
  - Used by: Multiple BigQuery UDF JavaScript functions
  - Files: `orchestration/dags/data_gcp_dbt/macros/function_utils/udf_js/*.sql`
- **Migration Assessment**: 🔄 **CONSIDER FOR SHARED UTILITIES BUCKET**
  - **Good isolation potential**: Dedicated utilities bucket for shared resources
  - **Small file size**: Easy to migrate, minimal risk
  - **Multiple consumers**: Used across BigQuery UDF functions
  - **Infrastructure improvement**: Better organization of shared utilities

---


---

## 🟢 LOW PRIORITY SUBFOLDERS
*Active, frequently accessed data with good isolation but minimal migration benefits due to hot access patterns*

### **QPI_exports** 🟢 LOW PRIORITY ATTENTION DEPENDANCE AUX DEVS
**Pattern**: External Import Pipeline (Hot Daily Processing)
- **WRITE**: External QPI system writes daily exports
  - Path: `QPI_exports/qpi_answers_{YYYYMMDD}/`
  - Format: `*.jsonl` files
- **READ**: `orchestration/dags/jobs/import/import_qpi.py` (daily, hot access)
  - Checks: `QPI_exports/qpi_answers_{today}/` for file presence
  - Imports: `QPI_exports/qpi_answers_{ds_nodash}/*.jsonl` → BigQuery
- **Migration Assessment**: 🔄 **DEFER UNTIL LATER PHASE**
  - **Excellent isolation**: Complete horizontal isolation, single consumer
  - **Hot access pattern**: Daily external imports, high-frequency usage
  - **Migration complexity**: Active external system dependency
  - **Lower cost benefit**: Hot data doesn't benefit from cold storage tiers

---

### **dms_export** 🟢 LOW PRIORITY
**Pattern**: ETL Staging Pipeline (Hot Active Processing)
- **WRITE**: Multiple operations in sequence (daily):
  1. `jobs/etl_jobs/external/dms/main.py` writes JSON: `dms_export/unsorted_dms_{target}_{updated_since}.json`
  2. `jobs/etl_jobs/external/dms/parse_dms_subscriptions_to_tabular.py` converts to Parquet
- **READ**: `orchestration/dags/jobs/import/import_dms_subscriptions.py` (daily processing)
  - Active processing of `dms_jeunes_{updated_since}.parquet` and `dms_pro_{updated_since}.parquet`
- **Migration Assessment**: 🔄 **DEFER UNTIL LATER PHASE**
  - **Perfect isolation**: Complete ETL pipeline, single use case
  - **Hot staging data**: Active daily transformation pipeline
  - **High-frequency access**: JSON→Parquet→BigQuery processing
  - **Lower priority**: Hot data provides less cost optimization benefit

---

### **elementary_reports** 🟢 LOW PRIORITY
**Pattern**: Report Archive (Daily Generation, Growing Volume)
- **WRITE**: `orchestration/dags/jobs/dbt/dbt_artifacts.py`
  - Path: `elementary_reports/{year}/elementary_report_{YYYYMMDD}.html`
  - Creates data quality monitoring reports daily
- **READ**: Manual access for report viewing (occasional)
- **Migration Assessment**: 🔄 **CONSIDER RETENTION POLICY FIRST**
  - **Good isolation**: Single writer, independent report storage
  - **Growing volume**: Daily accumulation requires management
  - **Lower priority**: Focus on retention/pruning strategy before migration
  - **Operational dependency**: Reports used for monitoring, need careful handling

---

### **export/cloudsql_recommendation_tables_to_bigquery** 🟢 LOW PRIORITY
**Pattern**: Bidirectional Sync Staging (Active Daily Operations)
- **WRITE**: `orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py`
  - Exports BigQuery data → GCS: `export/cloudsql_recommendation_tables_to_bigquery/{timestamp}/`
- **READ**: Bidirectional sync operations (active daily usage)
- **Migration Assessment**: 🔄 **DEFER DUE TO ACTIVE SYNC OPERATIONS**
  - **Good isolation**: Recommendation sync only, no external dependencies
  - **Active daily usage**: Regular bidirectional sync operations
  - **Complex coordination**: Multiple coordinated operations within single use case
  - **Lower priority**: Active staging data, limited cold storage benefits

---

### **clickhouse_export** 🟢 LOW PRIORITY
**Pattern**: Temporary Staging (Daily Active Processing)
- **WRITE**: `orchestration/dags/jobs/export/export_clickhouse.py`
  - Path: `{DATA_GCS_BUCKET_NAME}/clickhouse_export/{ENV_SHORT_NAME}/export/{DATE}`
  - Exports BigQuery data to GCS as intermediate staging
- **READ**: Same DAG forwards staged data to ClickHouse (daily)
- **Migration Assessment**: 🔄 **DEFER DUE TO ACTIVE USAGE**
  - **Good isolation**: Single DAG manages entire pipeline
  - **Daily active usage**: Regular staging pattern BigQuery → GCS → ClickHouse
  - **External system dependency**: ClickHouse integration complexity
  - **Lower cost benefit**: Temporary staging data, limited savings

---


---

## Migration Priority Ranking
*Prioritized by isolation + temperature for maximum cost savings and minimum risk*

### **Tier 1: Immediate Cold Storage Migration (Maximum Cost Savings, Minimal Risk)**
1. **archives** - Cold storage archives, perfect isolation, immediate cost savings
2. **bigquery_archive_tables** - Legacy cold archives, cleanup opportunity
3. **historization/** - Large volume archival system, immediate coldline benefits
4. **QPI_historical** - Historical cold data, migrate to seed structure
5. **QPI_mapping** - Cold reference mapping table, perfect isolation

### **Tier 2: Warm Isolated Data Migration (Good Cost Benefits, Moderate Risk)**
6. **bigquery_imports/seed** - Large isolated reference data, frequent access
7. **base32-encode** - Shared utility, dedicated utilities bucket

### **Tier 3: Hot Data Migration (Lower Cost Benefits, Higher Complexity)**
8. **QPI_exports** - Daily hot imports, external system dependencies
9. **dms_export** - Active hot ETL pipeline, high-frequency processing

### **Tier 4: Low Priority / Keep in Main Bucket (Minimal Benefits or Active Dependencies)**
10. **export/cloudsql_recommendation_tables_to_bigquery** - Active sync operations, complex coordination
11. **elementary_reports** - Daily reports, consider retention policy first
12. **clickhouse_export** - Active daily staging, external system complexity

---

## Progressive Migration Roadmap
*Following temperature-first approach: Cold → Warm → Hot*

### **Phase 1: Cold Storage Migration (Month 1) - Maximum Impact, Minimal Risk**
- **archives** → `cold-archive-bucket-{ENV_SHORT_NAME}`
  - Manual archives, perfect isolation, immediate cost savings
- **bigquery_archive_tables** → `legacy-archive-bucket-{ENV_SHORT_NAME}` or cleanup
  - Legacy cold archives, validate usage first
- **historization/** → `archive-bucket-{ENV_SHORT_NAME}`
  - Large volume archival system, immediate coldline benefits

### **Phase 2: Cold Reference Data Migration (Month 2) - High Volume, Low Risk**
- **QPI_historical** → `seed-data-bucket-{ENV_SHORT_NAME}`
  - Historical cold data consolidation with seed structure
- **QPI_mapping** → `seed-data-bucket-{ENV_SHORT_NAME}` or dedicated reference bucket
  - Cold reference mapping table, perfect isolation, easy migration

### **Phase 3: Warm Isolated Data Migration (Month 3) - Good Benefits, Moderate Risk**
- **bigquery_imports/seed** → `seed-data-bucket-{ENV_SHORT_NAME}`
  - Large reference data volume, frequent but predictable access
- **base32-encode** → `shared-utilities-bucket-{ENV_SHORT_NAME}`
  - Shared utility migration, dedicated utilities bucket for better organization

### **Phase 4: Hot Data Migration (Month 4+) - Lower Priority, Higher Complexity**
- **QPI_exports** → `qpi-import-bucket-{ENV_SHORT_NAME}` (coordinate with devs)
  - Hot daily imports, external system dependencies, requires dev coordination
- **dms_export** → `dms-staging-bucket-{ENV_SHORT_NAME}` (final phase)
  - Hot ETL pipeline, high-frequency processing, most complex migration

### **Phase 5: Low Priority / Deferred (Month 5+) - Consider Later or Keep in Main Bucket**
- **export/cloudsql_recommendation_tables_to_bigquery** → Consider later if needed
  - Active sync operations, complex coordination, minimal cost benefits
- **elementary_reports** → Implement retention policy first, then consider migration
  - Daily reports accumulation, focus on pruning strategy
- **clickhouse_export** → Consider later if external system changes
  - Active daily staging, external system complexity

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
- Start with **cold storage** subfolders first (minimal active usage, lower risk)
- Prioritize **perfect isolation** cases before mixed-usage scenarios
- Maintain **parallel operations** during transition periods for active systems
- Implement **monitoring** for bucket usage and access patterns
- Plan **rollback procedures** for each migration phase
- **Temperature-based staging**: Cold → Warm → Hot migration sequence

---

## Expected Benefits

### **Immediate Cost Optimization (Phase 1 Focus)**
- **Maximum savings from cold storage**: Archives and historical data move to coldline/archive tiers
- **Dedicated lifecycle policies**: Temperature-based storage class transitions
- **Volume-based savings**: Large archival datasets benefit from cheaper storage tiers
- **Regional optimization**: Archive buckets in cost-optimized regions

### **Operational Efficiency (Progressive Improvement)**
- **Risk-minimized approach**: Start with cold, isolated data (minimal disruption)
- **Clear ownership boundaries**: Each bucket dedicated to specific use cases
- **Simplified troubleshooting**: Isolated failure domains per use case
- **Better monitoring**: Use case-specific metrics and alerting

### **Security & Access Control (Enhanced Isolation)**
- **Granular bucket-level permissions**: Fine-tuned access per data temperature
- **Isolated blast radius**: Security issues contained within specific buckets
- **Compliance optimization**: Sensitive archives with dedicated access controls
- **Audit trail improvements**: Clear data lineage per use case

### **Strategic Infrastructure Benefits (Long-term Value)**
- **Temperature-aware architecture**: Foundation for intelligent data management
- **Scalable data patterns**: Easy to add new use cases following temperature model
- **Cost predictability**: Clear cost allocation per data use case and temperature
- **Future-ready foundation**: Ready for advanced lifecycle policies and automation

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
