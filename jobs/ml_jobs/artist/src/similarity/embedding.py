import pandas as pd
from sentence_transformers import SentenceTransformer

from src.common.constants import (
    ENCODER_NAME,
    HF_TOKEN_SECRET_NAME,
)
from src.common.gcp import get_secret


def embed_artist_biographies(artist_biographies: pd.Series) -> pd.Series:
    gemma_encoder = SentenceTransformer(
        ENCODER_NAME,
        token=get_secret(HF_TOKEN_SECRET_NAME),
    )
    PROMPT_NAME = "Clustering"
    BATCH_SIZE = 128

    embeddings = gemma_encoder.encode(
        artist_biographies.tolist(),
        show_progress_bar=True,
        batch_size=BATCH_SIZE,
        prompt_name=PROMPT_NAME,
    )
    return pd.Series(list(embeddings), index=artist_biographies.index)
