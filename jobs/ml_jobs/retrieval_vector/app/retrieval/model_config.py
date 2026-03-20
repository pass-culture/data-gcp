import json
from dataclasses import dataclass
from enum import StrEnum

from app.retrieval.client import DefaultClient
from app.retrieval.metadata_graph_client import MetadataGraphClient
from app.retrieval.reco_client import RecoClient


class ModelType(StrEnum):
    RECOMMENDATION = "recommendation"
    METADATA_GRAPH = "metadata_graph"


class VectorSearchMetric(StrEnum):
    DOT = "dot"
    COSINE = "cosine"


@dataclass
class ModelConfig:
    type: ModelType
    vector_search_metric: VectorSearchMetric


def load_model() -> DefaultClient:
    """Load the model based on the configuration file."""

    # Load model configuration
    with open("./metadata/model_type.json", "r") as file:
        desc = json.load(file)
    config = ModelConfig(
        type=ModelType(desc["type"]),
        vector_search_metric=VectorSearchMetric(desc["vector_search_metric"]),
    )

    if config.type == ModelType.RECOMMENDATION:
        return RecoClient(vector_search_metric=config.vector_search_metric)
    elif config.type == ModelType.METADATA_GRAPH:
        return MetadataGraphClient(vector_search_metric=config.vector_search_metric)
    else:
        raise ValueError("Invalid model type")
