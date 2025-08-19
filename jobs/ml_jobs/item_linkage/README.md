# Item Linkage Workflow

This directory contains the code and scripts for linking items (products and offers) using semantic vectors, orchestrated via Airflow and GCE.

[Detailed documentation is available on Notion](https://www.notion.so/passcultureapp/Items-Linkage-913d770e69b64f2880c98f51af447a65).

## Overview

The workflow is managed by the Airflow DAG `link_items.py`, which automates the following steps:

1. **Import Data**
   - SQL queries are run to import sources and candidates data into BigQuery temporary tables.

2. **Export Data to GCS**
   - Data from BigQuery tables is exported as Parquet files to Google Cloud Storage.

3. **Start GCE Instance & Install Dependencies**
   - A GCE VM is started.
   - The codebase is fetched and Python dependencies are installed.

4. **Preprocess Data**
   - Run `preprocess.py` to clean and batch sources and candidates data.

5. **Product Linkage Workflow**
   - Prepare product tables: `prepare_tables.py --linkage-type product`
   - Build semantic space: `build_semantic_space.py --linkage-type product`
   - Generate linkage candidates: `linkage_candidates.py --linkage-type product`
   - Link products: `link_items.py --linkage-type product`
   - Load linked products into BigQuery.
   - Evaluate linkage: `evaluate.py --linkage-type product`

6. **Offer Linkage Workflow**
   - Prepare offer tables: `prepare_tables.py --linkage-type offer`
   - Build semantic space: `build_semantic_space.py --linkage-type offer`
   - Generate linkage candidates: `linkage_candidates.py --linkage-type offer`
   - Link offers: `link_items.py --linkage-type offer`
   - Assign IDs: `assign_linked_ids.py`
   - Load linked offers into BigQuery.
   - Evaluate linkage: `evaluate.py --linkage-type offer`

7. **Export Item Mapping**
   - Run SQL to build and export item-offer mapping to BigQuery.

8. **Stop GCE Instance**

## How to Run

The recommended way to run the workflow is via Airflow, which will handle all orchestration and dependencies.
If you want to run individual scripts manually, follow the order above and use the same arguments as in the DAG.

### Example Commands

- Preprocess data:
  ```bash
  python preprocess.py --input-path <input_parquet> --output-path <output_dir> --reduction true --batch-size 100000
  ```

- Prepare tables:
  ```bash
  python prepare_tables.py --linkage-type product --input-candidates-path <candidates_parquet> --output-candidates-path <output_dir> --input-sources-path <sources_parquet> --output-sources-path <output_dir> --batch-size 100000
  ```

- Build semantic space:
  ```bash
  python build_semantic_space.py --input-path <input_parquet> --linkage-type product --batch-size 100000
  ```

- Generate linkage candidates:
  ```bash
  python linkage_candidates.py --batch-size 100000 --linkage-type product --input-path <input_parquet> --output-path <output_dir>
  ```

- Link items:
  ```bash
  python link_items.py --linkage-type product --input-sources-path <sources_dir> --input-candidates-path <candidates_dir> --linkage-candidates-path <candidates_dir> --output-path <output_dir> --unmatched-elements-path <unmatched_dir>
  ```

- Assign linked IDs (for offers):
  ```bash
  python assign_linked_ids.py --input-path <linked_offers_dir> --output-path <linked_w_id_dir>
  ```

- Evaluate linkage:
  ```bash
  python evaluate.py --input-candidates-path <candidates_dir> --linkage-path <linked_dir> --linkage-type product
  ```

## Notes

- All paths and parameters should match those defined in the DAG config.
- The workflow requires access to GCP resources (BigQuery, GCS, GCE).
- For production runs, use Airflow to ensure proper sequencing and resource management.

---

For more details, see the DAG definition in `link_items.py`.
