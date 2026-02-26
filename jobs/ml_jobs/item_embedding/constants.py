import os

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")

# Hugging Face
HF_TOKEN_SECRET_NAME = (
    "huggingface_token_prod" if ENV_SHORT_NAME == "prod" else "huggingface_token_ehp"
)
