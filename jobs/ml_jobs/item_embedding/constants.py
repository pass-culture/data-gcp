import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# Hugging Face token secret name per environment
_HF_TOKEN_SECRET_NAMES: dict[str, str] = {
    "prod": "huggingface_token_prod",
    "stg": "huggingface_token_ehp",
    "dev": "huggingface_token_ehp",
}
HF_TOKEN_SECRET_NAME = _HF_TOKEN_SECRET_NAMES.get(
    ENV_SHORT_NAME, "huggingface_token_ehp"
)

# HF constants
BATCH_SIZE = 32

# Prompts longer than this many tokens are silently truncated by the encoder
# (see SentenceTransformer.max_seq_length; 2048 for embeddinggemma-300m).
# Used as a fallback when an encoder doesn't expose max_seq_length (e.g. a
# test double); the real per-encoder value is preferred at runtime.
MAX_PROMPT_TOKENS = 2048

# Target number of rows per uniform input chunk when streaming the input
# dataset (see gcs_utils.iter_metadata_chunks). Distinct from BATCH_SIZE:
# this controls how many rows are read from GCS before a single
# embed_dataframe()/encode() call, independent of BigQuery's arbitrary,
# uneven per-file export sharding; BATCH_SIZE controls SentenceTransformer's
# internal batching within one encode() call.
ROWS_PER_CHUNK = 50_000
