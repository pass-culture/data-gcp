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
        # The empty prompt is kept in place so the result stays row-aligned
        # with the input; embed_dataframe is what drops the item later.
        df = pd.DataFrame({"x": [None], "y": [None]})
        prompts = _build_prompts(df, self.make_vector(["x", "y"]))
        assert prompts == [""]

    def test_empty_prompt_stays_in_position(self):
        # An all-null row in the middle must keep its slot so the list stays
        # aligned row-for-row with the DataFrame.
        df = pd.DataFrame({"x": ["hello", None, "world"]})
        prompts = _build_prompts(df, self.make_vector(["x"]))
        assert prompts == ["x : hello", "", "x : world"]

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

    def test_labels_override_column_names(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "author_concat": ["Herbert"]})
        vector = Vector(
            name="test",
            features=["offer_name", "author_concat"],
            encoder_name="model",
            labels={"offer_name": "titre", "author_concat": "auteur / artiste"},
        )
        prompts = _build_prompts(df, vector)
        assert prompts == ["titre : Dune\nauteur / artiste : Herbert"]

    def test_unmapped_feature_falls_back_to_column_name(self):
        df = pd.DataFrame({"offer_name": ["Dune"], "category_id": ["LIVRE"]})
        vector = Vector(
            name="test",
            features=["offer_name", "category_id"],
            encoder_name="model",
            labels={"offer_name": "titre"},
        )
        prompts = _build_prompts(df, vector)
        assert prompts == ["titre : Dune\ncategory_id : LIVRE"]


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

        result = embed_dataframe(df, vectors, encoders)

        assert "item_id" in result.columns
        assert "content_hash" in result.columns
        assert "emb" in result.columns
        assert len(result) == 3

        # Verify encoder.encode was called
        assert mock_encoder.encode.called

    def test_all_null_row_is_skipped_and_not_embedded(self):
        # Only the two non-empty rows should be embedded; the all-null row
        # must not be sent to the encoder and must be dropped from the output,
        # so no null vector ever reaches the parquet.
        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.return_value = np.array([[1.0, 2.0], [5.0, 6.0]])

        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "c"],
                "content_hash": ["h1", "h2", "h3"],
                "name": ["Alice", None, "Charlie"],
            }
        )
        vectors = [Vector(name="emb", features=["name"], encoder_name="test/model")]
        encoders = {"test/model": mock_encoder}

        result = embed_dataframe(df, vectors, encoders)

        # The all-null item ("b") is excluded; survivors keep their embeddings.
        assert result["item_id"].tolist() == ["a", "c"]
        assert result["emb"].tolist() == [[1.0, 2.0], [5.0, 6.0]]
        assert result["emb"].notna().all()

        # The empty prompt was never passed to the encoder.
        (called_prompts,), _ = mock_encoder.encode.call_args
        assert called_prompts == ["name : Alice", "name : Charlie"]

    def test_each_item_keeps_its_own_embedding(self):
        # The embedding an item ends up with must be the one built from *that*
        # item's prompt, even when a middle item is dropped and the input has a
        # non-default index.
        def encode_from_prompts(prompts, **kwargs):
            # Turn each prompt into a distinct, content-derived vector so any
            # mismatch between items and embeddings would show up.
            return np.array([[float(len(p)), float(ord(p[-1]))] for p in prompts])

        mock_encoder = MagicMock()
        mock_encoder.device = "cpu"
        mock_encoder.encode.side_effect = encode_from_prompts

        df = pd.DataFrame(
            {
                "item_id": ["a", "b", "c", "d"],
                "content_hash": ["h1", "h2", "h3", "h4"],
                "name": ["Alice", "Bob", None, "Dana"],
            },
            index=[10, 20, 30, 40],  # non-default index must not break alignment
        )
        vectors = [Vector(name="emb", features=["name"], encoder_name="test/model")]
        encoders = {"test/model": mock_encoder}

        result = embed_dataframe(df, vectors, encoders)

        # "c" is dropped; the survivors keep their order and identity.
        assert result["item_id"].tolist() == ["a", "b", "d"]

        # Each surviving item maps to the embedding built from its own prompt.
        expected = {
            "a": [float(len("name : Alice")), float(ord("e"))],
            "b": [float(len("name : Bob")), float(ord("b"))],
            "d": [float(len("name : Dana")), float(ord("a"))],
        }
        for item_id, embedding in zip(result["item_id"], result["emb"]):
            assert embedding == expected[item_id]
