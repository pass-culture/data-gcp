# Titelive ETL Scripts

This project contains ETL scripts for extracting and processing data from the Titelive API. Titelive is a French book and music distribution platform that provides metadata and commercial information about cultural products.

## Overview

The project consists of three main scripts that work together to extract,
process, and manage Titelive data:

1. **`extract_new_offers_from_titelive.py`** - Extracts raw data from the Titelive API
2. **`parse_offers.py`** - Processes and formats the extracted data
3. **`upload_titelive_images_to_gcs.py`** - Downloads and uploads product images to Google Cloud Storage

## Prerequisites

- Python 3.12+
- Access to Titelive API credentials (stored in GCP Secret Manager)
- Required dependencies (see `pyproject.toml`)

## Installation

We use uv to manage our Python environment and dependencies. To install the required packages, run:

```bash
make install
```

## Scripts

### 1. Extract New Offers (`extract_new_offers_from_titelive.py`)

This script extracts raw product data from the Titelive API based on modification date and product category.

#### Usage

```bash
python scripts/extract_new_offers_from_titelive.py \
  --offer-category LIVRE \
  --min-modified-date "2024-01-01" \
  --output-file-path "data/raw_offers.parquet"
```

#### Parameters

- `--offer-category`: Category of offers to extract
  - `LIVRE` (paper books)
  - `MUSIQUE_ENREGISTREE` (recorded music)
- `--min-modified-date`: Minimum modification date for offers (YYYY-MM-DD format)
- `--output-file-path`: Path where the extracted data will be saved (Parquet format)

#### Output

The script generates a Parquet file containing:

- `id`: Unique identifier for each product
- `data`: JSON string containing the raw API response for each product

#### Features

- Automatic token management with refresh capability
- Pagination handling for large result sets
- Rate limiting and error handling
- UTF-8 encoding support for French content
- Configurable results per page (default: 120)
- Maximum response limit protection

### 2. Parse Offers (`parse_offers.py`)

This script processes the raw data extracted by the first script, flattening the nested JSON structure and applying data transformations.

#### Usage

```bash
python scripts/parse_offers.py \
  --min-modified-date "2024-01-01" \
  --input-file-path "data/raw_offers.parquet" \
  --output-file-path "data/processed_offers.parquet"
```

#### Parameters

- `--min-modified-date`: Minimum modification date for filtering (YYYY-MM-DD format)
- `--input-file-path`: Path to the input Parquet file (output from extract script)
- `--output-file-path`: Path where the processed data will be saved

#### Processing Steps

1. **JSON Parsing**: Converts the JSON data column into structured DataFrame columns
2. **Article Explosion**: Flattens nested article data from the product records
3. **Column Prefixing**: Adds `article_` prefix to article-specific columns
4. **Date Filtering**: Filters records based on modification date
5. **Data Type Enforcement**: Ensures proper data types for specific columns
6. **JSON Serialization**: Converts complex objects (dicts, lists) to JSON strings
7. **Null Value Cleaning**: Standardizes null representations

#### Output Columns

The processed dataset includes columns such as:

- `titre`: Product title
- `auteurs_multi`: Authors information (JSON format)
- `article_*`: Article-specific attributes, which vary depending on the product type (LIVRE, MUSIQUE_ENREGISTREE)

### 3. Upload Titelive Images to GCS (`upload_titelive_images_to_gcs.py`)

This script downloads product images from Titelive and uploads them to Google
Cloud Storage. It processes both recto (front) and verso (back) images,
generating unique UUIDs for each image and creating GCS paths for storage.

#### Image Upload Usage

```bash
python scripts/upload_titelive_images_to_gcs.py \
  --input-parquet-path "data/processed_offers.parquet" \
  --gcs-thumb-base-path "gs://bucket-name/images/titelive" \
  --output-parquet-path "data/offers_with_images.parquet"
```

#### Image Upload Parameters

- `--input-parquet-path`: Path to the input Parquet file containing parsed
  Titelive data with `article_imagesUrl` column
- `--gcs-thumb-base-path`: Base GCS path where images will be uploaded
  (e.g., "gs://bucket-name/images/titelive")
- `--output-parquet-path`: Path where the enhanced data with image upload
  status will be saved

#### Image Processing Steps

1. **Image URL Extraction**: Parses the `article_imagesUrl` JSON column to
   extract recto and verso image URLs
2. **UUID Generation**: Creates unique UUIDs for each image to avoid naming
   conflicts
3. **GCS Path Construction**: Builds full GCS paths using the base path and
   generated UUIDs
4. **Image Download and Upload**: Downloads images from Titelive URLs and
   uploads them to GCS
5. **Status Tracking**: Records upload success/failure status for each image
6. **Data Merging**: Combines original data with image metadata and upload status

#### Image Output Columns

In addition to the original columns, the output includes:

- `recto`: Original recto (front) image URL
- `verso`: Original verso (back) image URL
- `recto_uuid`: Generated UUID for recto image
- `verso_uuid`: Generated UUID for verso image
- `recto_gcs_path`: Full GCS path for uploaded recto image
- `verso_gcs_path`: Full GCS path for uploaded verso image
- `recto_upload_status`: Upload status tuple (success, url, message) for recto
- `verso_upload_status`: Upload status tuple (success, url, message) for verso

#### Image Upload Features

- Handles missing image URLs gracefully
- Generates unique UUIDs to prevent filename conflicts
- Provides detailed upload status tracking
- Processes sample data (currently limited to first 5 rows)
- Supports both recto and verso images

## Project Structure

```
├── scripts/
│   ├── extract_new_offers_from_titelive.py  # Data extraction script
│   ├── parse_offers.py                      # Data processing script
│   └── upload_titelive_images_to_gcs.py     # Image upload script
├── src/
│   ├── constants.py                         # API configuration and constants
│   └── utils/
│       ├── gcp.py                          # GCP Secret Manager utilities
│       └── requests.py                     # API request handling
├── data/                                   # Data storage directory
├── pyproject.toml                         # Project dependencies
└── README.md                              # This file
```

## Configuration

### Environment Variables

- `GCP_PROJECT_ID`: Google Cloud Project ID (default: "passculture-data-ehp")

### Secrets (stored in GCP Secret Manager)

- `titelive_epagine_api_username`: Titelive API username
- `titelive_epagine_api_password`: Titelive API password

## API Details

- **Base URL**: `https://catsearch.epagine.fr/v1`
- **Authentication**: Bearer token obtained via login endpoint
- **Rate Limiting**: Built-in handling with configurable timeouts
- **Encoding**: UTF-8 for proper French character support

## Error Handling

All scripts include comprehensive error handling:

- Automatic token refresh on 401 errors
- Request timeout handling
- JSON parsing error management
- Data type conversion error handling
- Graceful handling of missing or malformed data
- Image download and upload error management (for the image upload script)

## Example Workflow

```bash
# Step 1: Extract raw data from Titelive API
python scripts/extract_new_offers_from_titelive.py \
  --offer-category LIVRE \
  --min-modified-date "2024-01-01" \
  --output-file-path "data/raw_books.parquet"

# Step 2: Process and format the data
python scripts/parse_offers.py \
  --min-modified-date "2024-01-01" \
  --input-file-path "data/raw_books.parquet" \
  --output-file-path "data/processed_books.parquet"

# Step 3: Upload product images to GCS
python scripts/upload_titelive_images_to_gcs.py \
  --input-parquet-path "data/processed_books.parquet" \
  --gcs-thumb-base-path "gs://your-bucket/images/titelive" \
  --output-parquet-path "data/books_with_images.parquet"
```

## Development

The project uses:

- **Ruff** for linting and code formatting
- **Typer** for CLI interface
- **Pandas** for data manipulation
- **Loguru** for logging
- **PyArrow** for Parquet file handling
- **Google Cloud Storage** for image storage and management

### Testing

The project includes a comprehensive automated test suite covering unit tests and integration tests.

#### Running Tests

```bash
# Install dependencies
make install

# Run all tests
make test

# Run only unit tests
make test-unit

# Run only integration tests
make test-integration

# Run tests with coverage report
make test-coverage
```

#### Test Structure

- **Unit Tests** (`tests/unit/`): Test individual functions and components in isolation
- **Integration Tests** (`tests/integration/`): Test complete workflows end-to-end
- **Test Data** (`tests/data/`): Sample API responses and test fixtures

For detailed testing documentation, see [`tests/README.md`](tests/README.md).

#### Continuous Integration

The project includes GitHub Actions CI/CD pipeline that:

- Runs tests on Python 3.12
- Performs code linting with ruff
- Generates coverage reports
- Validates both unit and integration tests

## Notes

- The scripts are designed to work with incremental data updates based on modification dates
- All dates should be provided in YYYY-MM-DD format but are converted to DD/MM/YYYY for the Titelive API
- The processed data maintains referential integrity between products and their articles
- JSON serialization is used for complex nested data structures to ensure compatibility with downstream systems
