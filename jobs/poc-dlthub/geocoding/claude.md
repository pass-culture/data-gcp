# Geopf Geocoding Pipeline

## Overview

This project implements a data pipeline using [dlthub](https://dlthub.com/) to geocode French addresses via the [Geopf API](https://data.geopf.fr/geocodage) and store results in DuckDB.

## Architecture

### Components

- **API**: Geopf geocoding service (`https://data.geopf.fr/geocodage/search/csv`)
- **Pipeline**: dlthub for data extraction and loading
- **Storage**: DuckDB database

### Pipeline Flow

1. Read CSV file with addresses
2. POST CSV to Geopf `/search/csv` endpoint
3. Parse geocoded CSV response
4. Load results into DuckDB table

## Project Structure

```
.
├── main.py                   # Pipeline implementation
├── sample_addresses.csv      # Test data
├── geopf_geocoding.duckdb   # DuckDB database (generated)
└── pyproject.toml           # Dependencies
```

## Key Files

### main.py

Contains three main components:

- `fetch_geocoded_data()`: Calls Geopf API and yields geocoded records
- `geocoded_addresses()`: dlt resource decorator for the data
- `run_pipeline()`: Pipeline orchestration

### sample_addresses.csv

Test CSV with French addresses for geocoding.

## Dependencies

- `dlt>=1.21.0` - Data pipeline framework
- `requests` - HTTP client for API calls
- `duckdb` - Embedded database

## Usage

Run the pipeline:

```bash
uv run python main.py
```

Query results:

```python
import duckdb
conn = duckdb.connect('geopf_geocoding.duckdb')
conn.execute('SELECT * FROM geocoding_data.geocoded_addresses').fetchall()
```

## API Details

**Endpoint**: POST `/search/csv`

**Parameters**:
- `data` (required): CSV file to geocode (max 50MB)
- `columns` (optional): Columns to use for geocoding
- `indexes` (optional): address, poi, parcel
- Filters: `citycode`, `postcode`, `departmentcode`, etc.

**Response**: CSV with original data plus geocoding results:
- `result_label`: Formatted address
- `result_score`: Confidence score
- `result_type`: Type (housenumber, municipality, etc.)
- `latitude`, `longitude`: Coordinates
- Additional metadata columns

## Pipeline Configuration

- **Pipeline name**: `geopf_geocoding`
- **Destination**: `duckdb`
- **Dataset**: `geocoding_data`
- **Table**: `geocoded_addresses`
- **Write disposition**: `replace` (overwrites on each run)

## Future Enhancements

- Add batch processing for large CSV files
- Implement incremental loading
- Add error handling and retry logic
- Support additional Geopf API parameters
- Add data validation and quality checks
