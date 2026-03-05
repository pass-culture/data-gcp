"""Unit tests for gcs_utils module."""

import pandas as pd
import pytest
from config import Vector
from gcs_utils import _validate_parquet_file, list_parquet_files, load_parquet_file


class TestLoadParquetFile:
    def test_load_valid_parquet(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        df = pd.DataFrame(
            {"item_id": ["a", "b"], "content_hash": ["h1", "h2"], "feat": [1, 2]}
        )
        df.to_parquet(path, index=False)

        vectors = [Vector(name="v", features=["feat"], encoder_name="model")]
        result = load_parquet_file(path, vectors)
        assert len(result) == 2
        assert set(result.columns) == {"item_id", "content_hash", "feat"}

    def test_load_missing_columns(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        pd.DataFrame({"item_id": [1]}).to_parquet(path, index=False)

        vectors = [Vector(name="v", features=["missing_col"], encoder_name="model")]
        with pytest.raises(ValueError, match="missing required columns"):
            load_parquet_file(path, vectors)


class TestListParquetFiles:
    def test_list_valid_path(self, tmp_path):
        # Note: This test only works with actual GCS paths
        # For local testing we just verify the validation
        with pytest.raises(ValueError, match="Invalid GCS path"):
            list_parquet_files("/local/path")

    def test_requires_gs_prefix(self):
        with pytest.raises(ValueError, match="Invalid GCS path"):
            list_parquet_files("bucket/path")


class TestValidateParquetFile:
    def test_all_columns_present(self):
        df = pd.DataFrame({"item_id": [1], "content_hash": ["h"], "feat": ["x"]})
        vectors = [Vector(name="v", features=["feat"], encoder_name="model")]
        _validate_parquet_file(df, vectors)  # Should not raise

    def test_missing_item_id(self):
        df = pd.DataFrame({"feat": [1]})
        vectors = [Vector(name="v", features=["feat"], encoder_name="model")]
        with pytest.raises(ValueError, match="missing required columns.*item_id"):
            _validate_parquet_file(df, vectors)

    def test_missing_feature_column(self):
        df = pd.DataFrame({"item_id": [1], "content_hash": ["h"]})
        vectors = [Vector(name="v", features=["missing"], encoder_name="model")]
        with pytest.raises(ValueError, match="missing required columns.*missing"):
            _validate_parquet_file(df, vectors)
