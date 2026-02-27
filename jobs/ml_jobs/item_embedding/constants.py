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
