import json
from dataclasses import dataclass
from typing import Optional

from app.retrieval.client import DefaultClient
from app.retrieval.metadata_graph_client import MetadataGraphClient
from app.retrieval.reco_client import RecoClient
from app.retrieval.text_client import TextClient


@dataclass
class ModelConfig:
    type: str
    default_token: Optional[str] = None
    transformer: Optional[str] = None
    reducer: Optional[str] = None


def load_model() -> DefaultClient:
    """Load the model based on the configuration file."""
    with open("./metadata/model_type.json", "r") as file:
        desc = json.load(file)
        config = ModelConfig(**desc)

        if config.type == "recommendation":
            return RecoClient(default_token=config.default_token)
        elif config.type == "semantic":
            return TextClient(
                transformer=config.transformer, reducer_path=config.reducer
            )
        elif config.type == "metadata_graph":
            return MetadataGraphClient(default_token=config.default_token)
        else:
            raise ValueError("Invalid model type")
