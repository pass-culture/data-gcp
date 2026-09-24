"""Unit tests for mlflow_utils pure helpers (no MLflow server needed)."""

from src.config import Vector
from src.mlflow_utils import _config_to_params


class TestConfigToParams:
    def test_full_vector_config(self):
        vector = Vector(
            name="movies_metadata",
            features=["offer_name", "offer_description"],
            encoder_name="google/embeddinggemma-300m",
            prompt_name="document",
            prompt_template='Ce film "{offer_name}".',
            labels={"offer_name": "titre", "offer_description": "description"},
            preprocessors={
                "offer_name": "normalize_whitespace",
                "offer_description": "clean_description",
            },
        )

        params = _config_to_params(vector.model_dump(exclude={"name"}), vector.name)

        assert params == {
            "movies_metadata.features": "offer_name, offer_description",
            "movies_metadata.encoder_name": "google/embeddinggemma-300m",
            "movies_metadata.prompt_name": "document",
            "movies_metadata.prompt_template": 'Ce film "{offer_name}".',
            "movies_metadata.labels": '{"offer_name": "titre", "offer_description": "description"}',
            "movies_metadata.preprocessors": '{"offer_name": "normalize_whitespace", "offer_description": "clean_description"}',
        }

    def test_empty_mappings_and_none_scalars(self):
        # labels/preprocessors default to {} and prompt_* to None: empty maps
        # contribute nothing; None scalars pass through (mlflow stringifies them).
        vector = Vector(
            name="v",
            features=["a"],
            encoder_name="m",
        )

        params = _config_to_params(vector.model_dump(exclude={"name"}), vector.name)

        assert params == {
            "v.features": "a",
            "v.encoder_name": "m",
            "v.prompt_name": None,
            "v.prompt_template": None,
        }
