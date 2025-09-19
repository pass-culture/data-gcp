# Bucket Subfolders Analysis by Use Case

## All Subfolders Found in `data-bucket-{env}`

### **Use Case: DMS Operations**
**Subfolder**: `/dms_export/`
- **PUSH**: `jobs/etl_jobs/external/dms/main.py`
  - Stores: `dms_export/unsorted_dms_{target}_{updated_since}.json`
- **PULL**: `orchestration/dags/jobs/import/import_dms_subscriptions.py`
  - Reads: `dms_export/dms_jeunes_*.parquet`
  - Reads: `dms_export/dms_pro_*.parquet`

---

### **Use Case: Recommendation Sync**
**Subfolder**: `/export/cloudsql_recommendation_tables_to_bigquery/`
- **BOTH**: `jobs/etl_jobs/internal/sync_recommendation/README.md`
  - Example paths: `gs://data-bucket-dev/export/cloudsql_recommendation_tables_to_bigquery/`
- **PULL**: `orchestration/dags/jobs/api/sync_cloudsql_recommendation_tables_to_bigquery.py`
- **PUSH**: `orchestration/dags/jobs/api/sync_bigquery_to_cloudsql_recommendation_tables.py`

---

### **Use Case: ClickHouse Export**
**Subfolder**: `/clickhouse_export/`
- **PUSH**: `orchestration/dags/jobs/export/export_clickhouse.py`
  - Sets `STORAGE_PATH` to `{DATA_GCS_BUCKET_NAME}/clickhouse_export/`

---

### **Use Case: ML Artist Linkage**
**Subfolder**: `/link_artists/`
- **PULL**: `jobs/ml_jobs/artist_linkage/streamlits/st_explore_and_clusterize_data.py`
  - Reads: `gs://data-bucket-stg/link_artists/artists_to_match.parquet`
- **PULL**: `jobs/ml_jobs/artist_linkage/streamlits/st_analyze_clustering.py`
  - Reads: `gs://data-bucket-stg/link_artists/matched_artists.parquet`

---

### **Use Case: GCS Seed Operations**
**Subfolder**: `/bigquery_imports/seed/` (with 50+ table subfolders)
- **PULL**: `jobs/etl_jobs/internal/gcs_seed/main.py`
  - Iterates through all REF_TABLES from `tables_config.py`
  - Reads from: `bigquery_imports/seed/{table_name}/{file_name}.{file_type}`
  - **Tables include**:
    - `qpi_mapping/` (parquet)
    - `macro_rayons/` (csv)
    - `eac_cash_in/` (csv)
    - `institution_metadata/` (csv)
    - `iris_france/` (csv)
    - `geo_iris/` (parquet)
    - `deps_qpv_2017/` (parquet)
    - `epci/` (avro)
    - `zrr/` (avro)
    - `qpv/` (avro)
    - `siren_main_business_labels/` (parquet)
    - `region_department/` (parquet)
    - And 40+ additional reference tables for geographic, demographic, and institutional data

---

### **Use Case: QPI Import Operations**
**Subfolders**: `/QPI_historical/` and `/QPI_exports/`
- **BOTH**: `orchestration/dags/jobs/import/import_qpi.py`
  - Reads historical data from: `QPI_historical/qpi_answers_historical_*.parquet`
  - Checks for daily exports in: `QPI_exports/qpi_answers_{YYYYMMDD}/`
  - Imports daily data from: `QPI_exports/qpi_answers_{ds_nodash}/*.jsonl`
  - Complete ETL cycle: external data import → staging → BigQuery raw tables

---

### **Use Case: BigQuery Partition Archiving**
**Subfolder**: `/historization/` (multiple subfolders)
- **PUSH**: `orchestration/dags/jobs/administration/bigquery_archive_partition.py` + `jobs/etl_jobs/internal/bigquery_archive_partition/`
  - Exports to: `gs://{DATA_GCS_BUCKET_NAME}/historization/{folder}/{table_id}/{partition}_{table_id}_*.parquet`
  - Subfolders used:
    - `/historization/tracking/` (for firebase_events table)
    - `/historization/int_firebase/` (for native_event tables)
    - `/historization/api_reco/` (for past_offer_context tables)
- **PULL**: `orchestration/dags/jobs/administration/bigquery_historize_applicative_database.py`
  - Reads parquet files from: `gs://{DATA_GCS_BUCKET_NAME}/historization/applicative/`

---

## **Additional Subfolders (Single File Usage)**

### **Use Case: Base32 JS Library**
**Subfolder**: `/base32-encode/`
- **PULL**: Referenced in `orchestration/dags/common/config.py`
  - Path: `gs://data-bucket-{env}/base32-encode/base32.js`
  - Used for JavaScript-based Base32 encoding operations in BigQuery UDFs

---

### **Use Case: ML Training and Models**
**Subfolder**: Various ML-related paths
- **BOTH**: `orchestration/dags/jobs/ml/artist_linkage.py`
- **BOTH**: `orchestration/dags/jobs/ml/link_items.py`
- **BOTH**: `orchestration/dags/jobs/ml/algo_training_*.py`
- **BOTH**: `orchestration/dags/jobs/ml/artist_wikidata_dump.py`
  - Uses both `DATA_GCS_BUCKET_NAME` and `ML_BUCKET_TEMP` for various ML operations

---

## **Use Cases Summary**

### **Complete Horizontal Use Cases (Clear Subfolder Isolation)**:
1. **GCS Seed Operations** (`/bigquery_imports/seed/`) - 1 main file with 50+ table subfolders - PULL only
2. **QPI Import Operations** (`/QPI_historical/`, `/QPI_exports/`) - 1 file - BOTH push & pull
3. **DMS Operations** (`/dms_export/`) - 2 files - BOTH push & pull
4. **Recommendation Sync** (`/export/cloudsql_recommendation_tables_to_bigquery/`) - 3 files - BOTH push & pull
5. **BigQuery Partition Archiving** (`/historization/`) - 2 files - BOTH push & pull
   - Uses multiple organized subfolders: `/tracking/`, `/int_firebase/`, `/api_reco/`, `/applicative/`
6. **ClickHouse Export** (`/clickhouse_export/`) - 1 file - PUSH only
7. **ML Artist Linkage** (`/link_artists/`) - 2 files - PULL only

### **Utility/Infrastructure Use Cases**:
- **Base32 JS Library** (`/base32-encode/`) - 1 file - PULL only

### **Mixed/Complex Use Cases**:
- **ML Training Operations** - Uses various paths, already has dedicated `ML_BUCKET_TEMP`

---

## **Migration Candidates by Use Case**

### **Best Candidates for Dedicated Buckets**:
1. **GCS Seed Operations**: Largest use case with 50+ isolated subfolders for reference data
2. **QPI Import Operations**: Complete ETL cycle with historical and daily export patterns
3. **BigQuery Partition Archiving**: Complete archival system with organized subfolders
4. **DMS Operations**: Complete ETL cycle with isolated subfolder
5. **Recommendation Sync**: Complete bidirectional sync with isolated subfolder
6. **ClickHouse Export**: Single-purpose export operation

### **Alternative Solutions**:
7. **ML Artist Linkage**: Can use existing `ML_BUCKET_TEMP` infrastructure

### **Infrastructure/Utility Use Cases**:
- **Base32 JS Library**: Small utility use case, likely better to keep in main bucket

### **Already Separated**:
- **Encrypted Export Operations**: Already uses dedicated `data-partners-export-bucket`
- **ML Training**: Already has dedicated `data-bucket-ml-temp` infrastructure

---

## **Separate Bucket Analysis**

### **Dedicated Export Bucket**: `data-partners-export-bucket-{ENV_SHORT_NAME}`
**Use Case: Encrypted Export Operations**
- **BOTH**: `orchestration/dags/jobs/dbt/dbt_encrypted_export.py`
  - Complete encrypted data export pipeline for external partners
  - Stores obfuscated BigQuery exports as encrypted parquet files
  - Handles partner-specific data transfers and encryption workflows

### **ML Temp Bucket**: `data-bucket-ml-temp-{ENV_SHORT_NAME}`
**Use Case: Machine Learning Operations**
- **BOTH**: Referenced in `orchestration/dags/common/config.py`
  - Dedicated temporary storage for ML workflows
  - Used by various ML jobs for model training artifacts and intermediate data
