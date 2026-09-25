"""Unit tests for config.py (single-vector YAML loading)."""

from unittest.mock import patch

import pytest
import yaml
from src.config import Vector, load_vector_config


class TestVector:
    def test_valid_vector(self):
        v = Vector(name="test", features=["a", "b"], encoder_name="model/name")
        assert v.name == "test"
        assert v.prompt_name is None
        assert v.preprocessors == {}

    def test_vector_with_all_fields(self):
        v = Vector(
            name="test",
            features=["a"],
            encoder_name="model/name",
            prompt_name="document",
            labels={"a": "label a"},
            prompt_template="{a}",
            preprocessors={"a": "normalize_whitespace"},
        )
        assert v.prompt_name == "document"
        assert v.labels == {"a": "label a"}

    def test_missing_required_field_raises(self):
        with pytest.raises(Exception):
            Vector(name="test", features=["a"])  # missing encoder_name

    def test_unknown_preprocessor_raises(self):
        with pytest.raises(ValueError, match="Unknown preprocessor"):
            Vector(
                name="test",
                features=["a"],
                encoder_name="model",
                preprocessors={"a": "does_not_exist"},
            )


class TestLoadVectorConfig:
    def _write(self, tmp_path, name, content):
        (tmp_path / f"{name}.yaml").write_text(yaml.dump(content), encoding="utf-8")

    def test_loads_single_vector(self, tmp_path):
        self._write(
            tmp_path,
            "movies_metadata",
            {"name": "movies_metadata", "features": ["a"], "encoder_name": "m"},
        )
        with patch("src.config.CONFIGS_PATH", tmp_path):
            vector = load_vector_config("movies_metadata")
        assert isinstance(vector, Vector)
        assert vector.name == "movies_metadata"

    def test_missing_file_raises(self, tmp_path):
        with patch("src.config.CONFIGS_PATH", tmp_path):
            with pytest.raises(FileNotFoundError):
                load_vector_config("nope")

    def test_invalid_yaml_raises(self, tmp_path):
        (tmp_path / "bad.yaml").write_text(": :\n  - :\n  invalid", encoding="utf-8")
        with patch("src.config.CONFIGS_PATH", tmp_path):
            with pytest.raises(yaml.YAMLError):
                load_vector_config("bad")

    def test_missing_required_key_raises(self, tmp_path):
        self._write(tmp_path, "incomplete", {"name": "v", "features": ["a"]})
        with patch("src.config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="missing required keys"):
                load_vector_config("incomplete")

    def test_non_mapping_raises(self, tmp_path):
        (tmp_path / "list.yaml").write_text(
            yaml.dump([{"name": "v"}]), encoding="utf-8"
        )
        with patch("src.config.CONFIGS_PATH", tmp_path):
            with pytest.raises(ValueError, match="must define a single vector"):
                load_vector_config("list")
