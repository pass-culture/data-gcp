import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Optional

from app.retrieval.client import DefaultClient
from app.retrieval.metadata_graph_client import MetadataGraphClient
from app.retrieval.reco_client import RecoClient


class ModelType(StrEnum):
    RECOMMENDATION = "recommendation"
    METADATA_GRAPH = "metadata_graph"


@dataclass
class ModelConfig:
    type: ModelType
    default_token: Optional[str] = None


def load_model() -> DefaultClient:
    """Load the model based on the configuration file."""

    # Load model configuration
    with open("./metadata/model_type.json", "r") as file:
        desc = json.load(file)
    config = ModelConfig(
        type=ModelType(desc["type"]), default_token=desc.get("default_token")
    )

    if config.type == ModelType.RECOMMENDATION:
        return RecoClient(default_token=config.default_token)
    elif config.type == ModelType.METADATA_GRAPH:
        return MetadataGraphClient(default_token=config.default_token)
    else:
        raise ValueError("Invalid model type")
