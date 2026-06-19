#!/usr/bin/env bash
# run_all_plots.sh — Generate all visualization plots for a given embeddings parquet.
#
# Output directory: results/plots_<embedding_basename>/
# Each plot script is run independently; errors are logged but do not stop the pipeline.
#
# Usage:
#   ./scripts/run_all_plots.sh <embeddings.parquet> --raw-data <raw_input_dir_or_parquet>
#
# Examples:
#   ./scripts/run_all_plots.sh results/item_embeddings_full.parquet \
#       --raw-data data/raw_input/book_music_catalog

set -euo pipefail

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <embeddings.parquet> --raw-data <path>"
  exit 1
fi

EMBEDDINGS_PATH="$1"
shift

RAW_DATA_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --raw-data) RAW_DATA_PATH="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Validate inputs
# ---------------------------------------------------------------------------
if [[ ! -f "$EMBEDDINGS_PATH" ]]; then
  echo "ERROR: embeddings file not found: $EMBEDDINGS_PATH" >&2
  exit 1
fi

if [[ -z "$RAW_DATA_PATH" ]]; then
  echo "ERROR: --raw-data is required (path to raw input parquet or directory)" >&2
  exit 1
fi

if [[ ! -e "$RAW_DATA_PATH" ]]; then
  echo "ERROR: raw-data path not found: $RAW_DATA_PATH" >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Resolve output directory
# ---------------------------------------------------------------------------
EMBEDDINGS_BASENAME=$(basename "$EMBEDDINGS_PATH" .parquet)
OUTPUT_DIR="results/plots_${EMBEDDINGS_BASENAME}"

if [[ -d "$OUTPUT_DIR" ]]; then
  echo ""
  echo "WARNING: output directory already exists: $OUTPUT_DIR"
  read -rp "  Overwrite existing plots? [y/N] " user_choice
  case "$user_choice" in
    [yY] | [yY][eE][sS])
      echo "  Overwriting existing plots in $OUTPUT_DIR"
      ;;
    *)
      echo "  Cancelled. No files were modified."
      exit 0
      ;;
  esac
else
  mkdir -p "$OUTPUT_DIR"
  echo "Created output directory: $OUTPUT_DIR"
fi

echo ""
echo "============================================"
echo "Embeddings : $EMBEDDINGS_PATH"
echo "Output dir : $OUTPUT_DIR"
echo "Raw data   : $RAW_DATA_PATH"
echo "============================================"

# ---------------------------------------------------------------------------
# Helper: run a Python plot script and log success or failure
# ---------------------------------------------------------------------------
run_plot_script() {
  local script_name="$1"
  shift
  echo ""
  echo "▶ $script_name"
  if uv run python "scripts/$script_name" "$@"; then
    echo "  ✓ done"
  else
    echo "  ✗ FAILED (see above)" >&2
  fi
}

# ---------------------------------------------------------------------------
# 1. Book vs Music item type separation (PCA)
# ---------------------------------------------------------------------------
run_plot_script plot_pca_item_type_separation.py \
  --embeddings "$EMBEDDINGS_PATH" \
  --raw-data   "$RAW_DATA_PATH" \
  --output     "$OUTPUT_DIR/pca_item_type_separation.png"

# ---------------------------------------------------------------------------
# 2. PCA colored by GTL level-1 genre
# ---------------------------------------------------------------------------
run_plot_script plot_pca_embeddings_by_gtl_level1.py \
  --embeddings "$EMBEDDINGS_PATH" \
  --raw-data   "$RAW_DATA_PATH" \
  --output     "$OUTPUT_DIR/pca_embeddings_by_gtl_level1.png"

# ---------------------------------------------------------------------------
# 3. PCA colored by GTL level-2 genre (books + music)
# ---------------------------------------------------------------------------
run_plot_script plot_pca_embeddings_by_gtl_level2.py \
  "$EMBEDDINGS_PATH" \
  --raw-data "$RAW_DATA_PATH" \
  --output   "$OUTPUT_DIR/pca_embeddings_by_gtl_level2.png"

# ---------------------------------------------------------------------------
# 4. Pairwise cosine distance heatmap between GTL cluster centroids
# ---------------------------------------------------------------------------
run_plot_script plot_gtl_cluster_distance_heatmap.py \
  "$EMBEDDINGS_PATH" \
  --raw-data "$RAW_DATA_PATH" \
  --plot     "$OUTPUT_DIR/gtl_cluster_distance_heatmap.png"

# ---------------------------------------------------------------------------
# 5. Hierarchical clustermap of GTL centroids (dendrogram)
# ---------------------------------------------------------------------------
run_plot_script plot_gtl_hierarchical_clustermap.py \
  "$EMBEDDINGS_PATH" \
  --raw-data "$RAW_DATA_PATH" \
  --output   "$OUTPUT_DIR/gtl_hierarchical_clustermap.png"

# ---------------------------------------------------------------------------
# 6. t-SNE projection of GTL cluster centroids
# ---------------------------------------------------------------------------
run_plot_script plot_tsne_gtl_centroids.py \
  "$EMBEDDINGS_PATH" \
  --raw-data "$RAW_DATA_PATH" \
  --output   "$OUTPUT_DIR/tsne_gtl_centroids.png"

# ---------------------------------------------------------------------------
# 7. PCA highlighting items linked to cross-type artists
# ---------------------------------------------------------------------------
run_plot_script plot_pca_cross_type_artist_items.py \
  --embeddings "$EMBEDDINGS_PATH" \
  --raw-data   "$RAW_DATA_PATH" \
  --output     "$OUTPUT_DIR/pca_cross_type_artist_items.png"

# ---------------------------------------------------------------------------
# 8. Metadata bridge node counts (book-only vs music-only vs bridge)
# ---------------------------------------------------------------------------
run_plot_script plot_metadata_bridge_node_counts.py \
  --raw-data   "$RAW_DATA_PATH" \
  --output-dir "$OUTPUT_DIR"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================================"
echo "All plots saved to: $OUTPUT_DIR"
ls "$OUTPUT_DIR"
echo "============================================"
