import pathlib

import pytest
import yaml

from src.common import config as config_module
from src.common.config import VocabularyConfig, apply_overrides, load_config


def test_load_default_config() -> None:
    config = load_config("default")
    assert config.split_level == 1
    assert config.min_corpus_items == 1000
    assert config.vocabulary.spacy_model == "fr_core_news_sm"
    assert config.clustering.min_cluster_size == "auto"
    assert config.labelling.reassign_outliers is False


def test_load_config_missing_file_raises(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config_module, "CONFIGS_PATH", tmp_path)
    with pytest.raises(FileNotFoundError):
        load_config("missing")


def test_load_config_non_mapping_raises(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config_module, "CONFIGS_PATH", tmp_path)
    (tmp_path / "bad.yaml").write_text(yaml.safe_dump(["not", "a", "mapping"]))
    with pytest.raises(ValueError, match="must define a mapping"):
        load_config("bad")


def test_load_config_missing_section_raises(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config_module, "CONFIGS_PATH", tmp_path)
    incomplete = {"language": "fr", "split_level": 1}
    (tmp_path / "incomplete.yaml").write_text(yaml.safe_dump(incomplete))
    with pytest.raises(ValueError, match="missing required top-level keys"):
        load_config("incomplete")


def test_load_config_empty_file_raises(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(config_module, "CONFIGS_PATH", tmp_path)
    (tmp_path / "empty.yaml").write_text("")
    with pytest.raises(ValueError, match="empty or invalid"):
        load_config("empty")


def test_apply_overrides_ignores_none_fields() -> None:
    base = load_config("default").vocabulary
    updated = apply_overrides(base, {"spacy_model": None, "min_description_chars": 10})

    assert updated.min_description_chars == 10
    assert updated.spacy_model == base.spacy_model
    assert isinstance(updated, VocabularyConfig)


def test_apply_overrides_returns_same_model_when_nothing_overridden() -> None:
    base = load_config("default").vocabulary
    updated = apply_overrides(base, {"spacy_model": None})
    assert updated == base
