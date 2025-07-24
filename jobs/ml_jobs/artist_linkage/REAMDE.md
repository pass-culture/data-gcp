# Artist Linkage

This project provides tools for linking products to artists using machine
learning and data matching techniques. It includes two main scripts for
different scenarios: creating artist-product links from scratch and updating
existing links with new products.

The documentation of this track is available in this **[Notion page](https://www.notion.so/passcultureapp/Artist-Linkage-9a83bb4e274c49e0894e2993e9f831e9)**.

## Overview

The artist linkage system helps match products (books, music, movies, etc.)
to their respective artists by:

- Using Wikidata for artist identification and enrichment
- Preprocessing artist names for better matching
- Creating clustering algorithms to group similar artist names
- Maintaining consistency across different product categories

## Main Scripts

### 1. `link_products_to_artists_from_scratch.py`

**Purpose**: Creates initial artist-product links when starting from an empty
database or completely rebuilding the linkage system.

**What it does**:

- Loads product data and filters it according to configured rules
- Preprocesses artist names for matching (removes common noise words like
  "multi-artistes", "compilation", etc.)
- Creates artist clusters by grouping products with similar artist names
  within the same category
- Matches new artist clusters against Wikidata to identify existing artists
- Generates three output tables:
  - `artist_df`: New artist entities with metadata
  - `artist_alias_df`: Artist name variations and aliases
  - `product_artist_link_df`: Product-to-artist mappings

**Input files**:

- `product_filepath`: Parquet file containing product data
- `wiki_base_path` + `wiki_file_name`: Wikidata extraction for artist matching

**Output files**:

- `output_artist_file_path`: Artists table
- `output_artist_alias_file_path`: Artist aliases table
- `output_product_artist_link_filepath`: Product-artist links table

**Usage**:

```bash
python link_products_to_artists_from_scratch.py \
  --product-filepath /path/to/products.parquet \
  --wiki-base-path /path/to/wiki/ \
  --wiki-file-name wikidata_artists.parquet \
  --output-artist-file-path /path/to/output_artists.parquet \
  --output-artist-alias-file-path /path/to/output_aliases.parquet \
  --output-product-artist-link-filepath /path/to/output_links.parquet
```

### 2. `link_new_products_to_artists.py`

**Purpose**: Incremental updates to existing artist-product links when new
products are added to the system.

**What it does**:

- Compares current products against existing product-artist links
- Identifies products to remove (no longer in current product set)
- Identifies new products that need to be linked to artists
- Attempts to match new products to existing artists using:
  - Raw artist name matching
  - Preprocessed artist name matching
- For unmatched products, creates new artist clusters and matches against
  Wikidata
- Generates delta tables (changes only) for incremental updates

**Input files**:

- `artist_filepath`: Existing artists table
- `artist_alias_file_path`: Existing artist aliases table
- `product_artist_link_filepath`: Current product-artist links
- `product_filepath`: Current products data
- `wiki_base_path` + `wiki_file_name`: Wikidata extraction

**Output files**:

- `output_delta_artist_file_path`: New artists to add
- `output_delta_artist_alias_file_path`: New artist aliases to add
- `output_delta_product_artist_link_filepath`: Product link changes
  (additions/removals)

**Usage**:

```bash
python link_new_products_to_artists.py \
  --artist-filepath /path/to/existing_artists.parquet \
  --artist-alias-file-path /path/to/existing_aliases.parquet \
  --product-artist-link-filepath /path/to/existing_links.parquet \
  --product-filepath /path/to/current_products.parquet \
  --wiki-base-path /path/to/wiki/ \
  --wiki-file-name wikidata_artists.parquet \
  --output-delta-artist-file-path /path/to/new_artists.parquet \
  --output-delta-artist-alias-file-path /path/to/new_aliases.parquet \
  --output-delta-product-artist-link-filepath /path/to/link_changes.parquet
```

## Key Features

### Data Preprocessing

- Filters out invalid artist names (configured in
  `artist_linkage_config.json`)
- Normalizes artist names for better matching
- Handles multi-artist scenarios

### Wikidata Integration

- Matches artist clusters against Wikidata for entity resolution
- Enriches artist data with additional metadata
- Ensures consistency with external knowledge bases

### Quality Assurance

Both scripts include comprehensive sanity checks:

- Validates all products are successfully linked to artists
- Prevents duplicate entries in output tables
- Ensures new artists don't conflict with existing ones
- Validates artist aliases don't already exist

### Incremental Processing

The incremental script (`link_new_products_to_artists.py`) efficiently
handles:

- Product additions
- Product removals
- Artist alias expansion
- Minimal data processing for better performance

## Configuration

### `artist_linkage_config.json`

Contains preprocessing rules, particularly artist names to filter out:

```json
{
    "preprocessing": {
        "artist_names_to_remove": [
            "multi-artistes", "compilation", "divers", "tbc", ...
        ]
    }
}
```

## Dependencies

Key Python packages (see `requirements.txt` for full list):

- `pandas`: Data manipulation
- `typer`: CLI interface
- `loguru`: Logging
- Custom utilities for matching, preprocessing, and data loading

## Project Structure

```text
├── link_products_to_artists_from_scratch.py  # Full rebuild script
├── link_new_products_to_artists.py          # Incremental update script
├── constants.py                              # Configuration constants
├── artist_linkage_config.json               # Preprocessing configuration
├── evaluate.py                               # Evaluation utilities
├── extract_from_wikidata.py                  # Wikidata extraction script
├── get_wikimedia_commons_license.py          # License extraction utilities
├── cluster_deprecated.py                     # Legacy clustering code
├── pyproject.toml                            # Project configuration
├── requirements.txt                          # Python dependencies
├── requirements.in                           # Requirements source
├── Makefile                                  # Build and test commands
├── __init__.py                               # Package initialization
├── queries/                                  # SPARQL queries for data extraction
│   ├── extract_book_artists.rq              # Books artist extraction
│   ├── extract_gkg_artists.rq               # GKG artist extraction
│   ├── extract_movie_artists.rq             # Movies artist extraction
│   └── extract_music_artists.rq             # Music artist extraction
├── utils/                                    # Utility modules
│   ├── __init__.py                          # Package initialization
│   ├── matching.py                          # Artist matching logic
│   ├── preprocessing_utils.py               # Data preprocessing
│   ├── loading.py                           # Data loading utilities
│   ├── clustering_utils.py                  # Clustering algorithms
│   ├── gcs_utils.py                         # Google Cloud Storage utilities
│   ├── mlflow.py                            # MLflow integration
│   └── constants.py                         # Utility constants
├── streamlits/                              # Streamlit apps for analysis
│   ├── st_analyze_clustering.py             # Clustering analysis app
│   ├── st_analyze_preprocessing.py          # Preprocessing analysis app
│   ├── st_create_preprocessing_test_set.py  # Test set creation app
│   ├── st_explore_and_clusterize_data.py    # Data exploration app
│   ├── st_investigate_syncho_backend.py     # Backend investigation app
│   ├── st_create_test_set/                  # Test set creation utilities
│   └── data/                                # Streamlit data files
└── tests/                                   # Unit tests
```

## Testing

Run tests using:

```bash
make test
```

## When to Use Which Script

- **Use `link_products_to_artists_from_scratch.py`** when:
  - Setting up the artist linkage system for the first time
  - Completely rebuilding the artist-product relationships
  - Testing new matching algorithms on the full dataset

- **Use `link_new_products_to_artists.py`** when:
  - Adding new products to an existing system
  - Regular incremental updates
  - Maintaining an operational artist linkage system
