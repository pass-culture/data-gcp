"""Unit tests for the item_embedding job."""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import yaml
from config import Vector, _load_config, parse_vectors
from embedding import (
    _build_prompts,
    embed_dataframe,
)


# ---------------------------------------------------------------------------
# Vector model tests
# ---------------------------------------------------------------------------
class TestVector:
    def test_valid_vector(self):
        v = Vector(name="test", features=["a", "b"], encoder_name="model/name")
        assert v.name == "test"
        assert v.prompt_name is None

    def test_vector_with_prompt_name(self):
        v = Vector(
            name="test",
            features=["a"],
            encoder_name="model/name",
            prompt_name="STS",
        )
        assert v.prompt_name == "STS"

    def test_vector_missing_required_field(self):
        with pytest.raises(Exception):
            Vector(name="test", features=["a"])  # missing encoder_name


# ---------------------------------------------------------------------------
# Config loading tests
# ---------------------------------------------------------------------------
class TestLoadConfig:
    def test_load_missing_file(self, tmp_path):
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(FileNotFoundError):
                _load_config("nonexistent")

    def test_load_invalid_yaml(self, tmp_path):
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text(": :\n  - :\n  invalid", encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(yaml.YAMLError):
                _load_config("bad")

    def test_load_valid_config(self, tmp_path):
        config_content = {
            "vectors": [
                {
                    "name": "test_vec",
                    "features": ["col_a"],
                    "encoder_name": "test/model",
                }
            ]
        }
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump(config_content), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            config = _load_config("test")
        assert "vectors" in config

    def test_load_config_missing_vectors_key(self, tmp_path):
        config_file = tmp_path / "no_vectors.yaml"
        config_file.write_text(yaml.dump({"other_key": 123}), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="missing required keys"):
                _load_config("no_vectors")


class TestParseVectors:
    def test_parse_valid(self, tmp_path):
        config_content = {
            "vectors": [
                {
                    "name": "v1",
                    "features": ["a", "b"],
                    "encoder_name": "model/x",
                }
            ]
        }
        config_file = tmp_path / "test.yaml"
        config_file.write_text(yaml.dump(config_content), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            vectors = parse_vectors("test")
        assert len(vectors) == 1
        assert vectors[0].name == "v1"

    def test_parse_empty_vectors(self, tmp_path):
        config_file = tmp_path / "empty.yaml"
        config_file.write_text(yaml.dump({"vectors": []}), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="No vectors configured"):
                parse_vectors("empty")

    def test_parse_no_vectors_key(self, tmp_path):
        config_file = tmp_path / "no_vectors.yaml"
        config_file.write_text(yaml.dump({}), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="missing required keys"):
                parse_vectors("no_vectors")

    def test_parse_invalid_vectors_type(self, tmp_path):
        config_file = tmp_path / "invalid.yaml"
        config_file.write_text(yaml.dump({"vectors": "not_a_list"}), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="must be a list"):
                parse_vectors("invalid")


# ---------------------------------------------------------------------------
# Prompt building tests
# ---------------------------------------------------------------------------
class TestBuildPrompts:
    def make_vector(self, features):
        return Vector(name="test", features=features, encoder_name="model")

    def test_basic(self):
        df = pd.DataFrame({"x": ["hello"], "y": ["world"]})
        prompts = _build_prompts(df, self.make_vector(["x", "y"]))
        assert prompts == ["x : hello\ny : world"]

    def test_null_feature_skipped(self):
        df = pd.DataFrame({"x": ["hello"], "y": [None]})
        prompts = _build_prompts(df, self.make_vector(["x", "y"]))
        assert prompts == ["x : hello"]

    def test_all_null_produces_empty_string(self):
        df = pd.DataFrame({"x": [None], "y": [None]})
        prompts = _build_prompts(df, self.make_vector(["x", "y"]))
        assert prompts == [""]

    def test_no_double_spaces_with_middle_null(self):
        df = pd.DataFrame({"a": ["v1"], "b": [None], "c": ["v3"]})
        prompts = _build_prompts(df, self.make_vector(["a", "b", "c"]))
        assert "\n\n" not in prompts[0]
        assert prompts[0] == "a : v1\nc : v3"

    def test_multiple_rows(self):
        df = pd.DataFrame({"x": ["a", "b", "c"]})
        prompts = _build_prompts(df, self.make_vector(["x"]))
        assert len(prompts) == 3
        assert prompts[1] == "x : b"


# ---------------------------------------------------------------------------
# End-to-end embed_dataframe tests
# ---------------------------------------------------------------------------
class TestEmbedDataframe:
    def test_end_to_end(self):
        # Mock encoder that returns deterministic embeddings
        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.return_value = np.array(
            [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        )

        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "c"],
                "content_hash": ["h1", "h2", "h3"],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        vectors = [Vector(name="emb", features=["name"], encoder_name="test/model")]
        encoders = {"test/model": mock_encoder}

        result = embed_dataframe(df, vectors, encoders, gpu_count=0)

        assert "item_id" in result.columns
        assert "content_hash" in result.columns
        assert "emb" in result.columns
        assert len(result) == 3

        # Verify encoder.encode was called
        assert mock_encoder.encode.called
