import json
from dataclasses import dataclass
from enum import StrEnum

from app.retrieval.client import DefaultClient
from app.retrieval.metadata_graph_client import MetadataGraphClient
from app.retrieval.reco_client import RecoClient


class ModelType(StrEnum):
    RECOMMENDATION = "recommendation"
    METADATA_GRAPH = "metadata_graph"


@dataclass
class ModelConfig:
    type: ModelType


def load_model() -> DefaultClient:
    """Load the model based on the configuration file."""

    # Load model configuration
    with open("./metadata/model_type.json", "r") as file:
        desc = json.load(file)
    config = ModelConfig(type=ModelType(desc["type"]))

    if config.type == ModelType.RECOMMENDATION:
        return RecoClient()
    elif config.type == ModelType.METADATA_GRAPH:
        return MetadataGraphClient()
    else:
        raise ValueError("Invalid model type")
