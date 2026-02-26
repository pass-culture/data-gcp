"""Unit tests for the item_embedding job."""

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest
import yaml
from config import Vector, load_config, parse_vectors
from embed_items import (
    _build_prompts,
    _validate_required_features,
    embed_all_vectors,
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
                load_config("nonexistent")

    def test_load_invalid_yaml(self, tmp_path):
        bad_file = tmp_path / "bad.yaml"
        bad_file.write_text(": :\n  - :\n  invalid", encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(yaml.YAMLError):
                load_config("bad")

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
            config = load_config("test")
        assert "vectors" in config

    def test_load_config_missing_vectors_key(self, tmp_path):
        config_file = tmp_path / "no_vectors.yaml"
        config_file.write_text(yaml.dump({"other_key": 123}), encoding="utf-8")
        with patch("config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="missing required keys"):
                load_config("no_vectors")


class TestParseVectors:
    def test_parse_valid(self):
        config = {
            "vectors": [
                {
                    "name": "v1",
                    "features": ["a", "b"],
                    "encoder_name": "model/x",
                }
            ]
        }
        vectors = parse_vectors(config)
        assert len(vectors) == 1
        assert vectors[0].name == "v1"

    def test_parse_empty_vectors(self):
        with pytest.raises(ValueError, match="No vectors configured"):
            parse_vectors({"vectors": []})

    def test_parse_no_vectors_key(self):
        with pytest.raises(ValueError, match="No vectors configured"):
            parse_vectors({})

    def test_parse_invalid_vectors_type(self):
        with pytest.raises(ValueError, match="must be a list"):
            parse_vectors({"vectors": "not_a_list"})


# ---------------------------------------------------------------------------
# Feature validation tests
# ---------------------------------------------------------------------------
class TestValidateRequiredFeatures:
    def test_all_present(self):
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        _validate_required_features(df, ["a", "b"])  # should not raise

    def test_missing_features(self):
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Missing required features.*b, c"):
            _validate_required_features(df, ["a", "b", "c"])


# ---------------------------------------------------------------------------
# Prompt building tests
# ---------------------------------------------------------------------------
class TestBuildPrompts:
    def make_vector(self, features):
        return Vector(name="test", features=features, encoder_name="model")

    def test_basic(self):
        df = pd.DataFrame({"x": ["hello"], "y": ["world"]})
        prompts = _build_prompts(df, self.make_vector(["x", "y"]))
        assert prompts == ["x : hello y : world"]

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
        assert "  " not in prompts[0]
        assert prompts[0] == "a : v1 c : v3"

    def test_multiple_rows(self):
        df = pd.DataFrame({"x": ["a", "b", "c"]})
        prompts = _build_prompts(df, self.make_vector(["x"]))
        assert len(prompts) == 3
        assert prompts[1] == "x : b"


# ---------------------------------------------------------------------------
# End-to-end embed_all_vectors tests
# ---------------------------------------------------------------------------
class TestEmbedAllVectors:
    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["item_id", "feat"])
        vectors = [Vector(name="v", features=["feat"], encoder_name="m")]
        with pytest.raises(ValueError, match="Empty metadata"):
            embed_all_vectors(df, vectors)

    def test_missing_item_id(self):
        df = pd.DataFrame({"feat": [1, 2]})
        vectors = [Vector(name="v", features=["feat"], encoder_name="m")]
        with pytest.raises(ValueError, match="item_id"):
            embed_all_vectors(df, vectors)

    def test_missing_feature_column(self):
        df = pd.DataFrame({"item_id": [1], "a": [1]})
        vectors = [Vector(name="v", features=["a", "missing"], encoder_name="m")]
        with pytest.raises(ValueError, match="Missing required features"):
            embed_all_vectors(df, vectors)

    @patch("embed_items._load_encoders")
    @patch("embed_items._get_gpu_count", return_value=0)
    def test_end_to_end(self, mock_gpu, mock_load_encoders):
        # Mock encoder that returns deterministic embeddings
        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.return_value = np.array(
            [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]
        )
        mock_load_encoders.return_value = {"test/model": mock_encoder}

        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "c"],
                "name": ["Alice", "Bob", "Charlie"],
            }
        )
        vectors = [Vector(name="emb", features=["name"], encoder_name="test/model")]

        result = embed_all_vectors(df, vectors, batch_size=32)

        assert list(result.columns) == ["item_id", "emb"]
        assert len(result) == 3
        # Each embedding should be a 1D array of shape (2,)
        assert result["emb"].iloc[0].shape == (2,)
        np.testing.assert_array_equal(result["emb"].iloc[0], [1.0, 2.0])

        # Verify encoder.encode was called with correct prompts
        call_args = mock_encoder.encode.call_args
        assert call_args.kwargs["batch_size"] == 32
