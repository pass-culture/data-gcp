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

# Target number of rows per uniform input chunk when streaming the input
# dataset (see gcs_utils.iter_metadata_chunks). Distinct from BATCH_SIZE:
# this controls how many rows are read from GCS before a single
# embed_dataframe()/encode() call, independent of BigQuery's arbitrary,
# uneven per-file export sharding; BATCH_SIZE controls SentenceTransformer's
# internal batching within one encode() call.
ROWS_PER_CHUNK = 50_000
# Cap the tokenized prompt length. embeddinggemma-300m defaults to 2048,
# but only ~2% of items exceed 512.
# Cap applied to every loaded encoder's max_seq_length (see
# setup_encoders.load_encoders), overriding the model's native default
# (2048 for embeddinggemma-300m). encode() sorts prompts by length before
# batching, so batches of long prompts each pad to the batch's own longest
# sequence; left uncapped, a batch of near-max-length prompts can OOM the
# GPU. A 50k-item catalogue audit found only ~2% of prompts exceed 512
# tokens, and truncated-vs-full embedding cosine similarity for that 2% is
# high (median 0.992, p5 0.963, p1 0.94), so the quality cost is small.
# Also used by LongPromptTracker as a fallback when an encoder doesn't
# expose max_seq_length (e.g. a test double); the real per-encoder value
# (which load_encoders sets to this same constant) is preferred at runtime.
MAX_SEQ_LENGTH = 512
