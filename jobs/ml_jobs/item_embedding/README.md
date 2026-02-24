# Item Embedding Microservice

A high-performance microservice for generating embeddings from item metadata using sentence transformers.

## Features

- **Batch Processing**: Efficiently processes items in configurable batches for optimal performance
- **Multiple Vector Types**: Supports multiple embedding configurations from a single input dataset
- **Flexible Configuration**: YAML-based configuration for easy customization
- **Progress Tracking**: Real-time progress bars and detailed logging
- **Error Handling**: Comprehensive error handling with informative messages

## Performance Enhancements

This version includes significant performance improvements:

### Batch Encoding (10-100x speedup)
- **Before**: Each item was encoded individually using `df.apply()`, making O(n) encoder calls
- **After**: Items are batched and encoded together, making O(n/batch_size) encoder calls
- **Impact**: For 10,000 items with batch_size=100, this reduces encoder calls from 10,000 to 100

### Memory Optimization
- Uses NumPy arrays for efficient intermediate storage
- Minimizes data type conversions
- Processes data in-place when possible

### GPU Utilization
- Batch processing better utilizes GPU resources
- Reduces CPU-GPU memory transfer overhead

## Installation

```bash
# Using uv (recommended)
uv sync

# Or using pip
pip install -e .
```

## Usage

### Basic Usage

```bash
python main.py \
  --input-parquet-filename input_items.parquet \
  --output-parquet-filename output_embeddings.parquet
```

### With Custom Configuration

```bash
python main.py \
  --config-file-name my_config \
  --batch-size 200 \
  --input-parquet-filename input_items.parquet \
  --output-parquet-filename output_embeddings.parquet
```

### Parameters

- `--config-file-name`: Name of the YAML config file in `configs/` directory (default: "default")
- `--batch-size`: Number of items to process per batch (default: 100)
  - Larger batches = faster processing but more memory
  - Recommended: 32-256 depending on GPU memory
- `--input-parquet-filename`: Path to input parquet file (required)
- `--output-parquet-filename`: Path to output parquet file (required)

## Configuration

Create a YAML file in the `configs/` directory:

```yaml
vectors:
  - name: "semantic_content"
    features:
      - offer_name
      - category_id
      - subcategory_id
      - description
    encoder_name: "google/embeddinggemma-300m"
    prompt_name: "STS"  # Optional

  - name: "visual_features"
    features:
      - image_url
    encoder_name: "clip-ViT-B-32"
```

### Configuration Options

- `name`: Unique identifier for the embedding vector
- `features`: List of column names from input DataFrame to concatenate
- `encoder_name`: HuggingFace model name or path
- `prompt_name`: Optional prompt template name (model-dependent)

## Input Format

Input parquet file should contain:
- All columns referenced in `features` configuration
- Any additional metadata columns (will be preserved in output)

## Output Format

Output parquet file contains:
- All original columns from input
- New columns: `{vector_name}_embedding` containing embedding arrays

## Performance Tips

1. **Batch Size**:
   - Start with 100 and increase if you have GPU memory available
   - Monitor GPU memory usage and adjust accordingly
   - Larger batches are almost always faster

2. **GPU Usage**:
   - Ensure CUDA is available for significant speedup
   - Use `nvidia-smi` to monitor GPU utilization

3. **Data Preprocessing**:
   - Clean/normalize text data before embedding
   - Remove or handle null values in feature columns

4. **Model Selection**:
   - Smaller models (e.g., 100M parameters) are faster but less accurate
   - Consider distilled models for production use

## Example Workflow

```python
import pandas as pd

# Prepare input data
df = pd.DataFrame({
    'offer_id': [1, 2, 3],
    'offer_name': ['Concert', 'Book', 'Movie'],
    'category_id': ['music', 'literature', 'cinema'],
    'description': ['Live music event', 'Fiction novel', 'Action film']
})

# Save to parquet
df.to_parquet('input.parquet', index=False)

# Run embedding
# python main.py --input-parquet-filename input.parquet --output-parquet-filename output.parquet

# Load results
df_embeddings = pd.read_parquet('output.parquet')
print(df_embeddings.columns)
# ['offer_id', 'offer_name', 'category_id', 'description', 'semantic_content_embedding']
```

## Troubleshooting

### Out of Memory Error
- Reduce `batch_size`
- Process data in chunks
- Use a smaller model

### Slow Processing
- Increase `batch_size`
- Verify GPU is being used
- Check input data quality (null values, very long texts)

### Missing Features Error
- Verify all columns in `features` exist in input data
- Check for typos in configuration

## Architecture

```
main.py
├── Loads configuration
├── Reads input parquet
└── Calls embed_features()

embed_items.py
├── create_prompts_from_features() - Batch prompt creation
├── encode_batch() - Batch encoding with SentenceTransformer
└── embed_features() - Orchestrates batch processing for each vector
```

## Dependencies

- pandas: DataFi manipulation
- sentence-transformers: Embedding models
- loguru: Logging
- typer: CLI interface
- pydantic: Configuration validation
- pyyaml: Configuration loading
- numpy: Efficient array operations
