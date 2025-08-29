import logging
from typing import List

import numpy as np
from langchain_google_vertexai import VertexAIEmbeddings
from langchain_huggingface import HuggingFaceEmbeddings

from utils.constants import EMBEDDING_MODELS

logger = logging.getLogger(__name__)


class EmbeddingModelManager:
    """Manages different embedding models for semantic search using LangChain."""

    def __init__(self) -> None:
        self.models: dict[str, object] = {}
        self._initialize_models()

    def _initialize_models(self) -> None:
        """Initialize embedding models with LangChain."""
        for langchain_class_name, model_name_list in EMBEDDING_MODELS.items():
            if langchain_class_name == "VertexAIEmbeddings":
                for model_name in model_name_list:
                    embeddings = VertexAIEmbeddings(model=model_name)
                    self.models[model_name] = embeddings
            elif langchain_class_name == "HuggingFaceEmbeddings":
                for model_name in model_name_list:
                    embeddings = HuggingFaceEmbeddings(model_name=model_name)
                    self.models[model_name] = embeddings
            else:
                logger.error(
                    "EMBEDDING_MODELS contains unsupported langchain modules: %s",
                    langchain_class_name,
                )

    def get_available_models(self) -> List[str]:
        """Get list of available model names."""
        return list(self.models.keys())

    def encode_items(self, texts: List[str], model_name: str) -> np.ndarray:
        """Encode texts using the specified model.
        Returns a list of embeddings where each embedding is a list[float].
        """
        if model_name not in self.models:
            raise ValueError(f"Model {model_name} not found")

        embeddings = self.models[model_name]
        # LangChain returns List[List[float]] for embed_documents
        return embeddings.embed_documents(texts)


# Global instance
embedding_manager = EmbeddingModelManager()
