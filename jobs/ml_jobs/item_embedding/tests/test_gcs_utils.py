"""Unit tests for gcs_utils module."""

import pandas as pd
import pytest
from gcs_utils import load_parquet, upload_parquet


class TestLoadParquet:
    def test_load_valid_parquet(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        df = pd.DataFrame({"item_id": ["a", "b"], "feat": [1, 2]})
        df.to_parquet(path, index=False)

        result = load_parquet(path)
        assert len(result) == 2
        assert list(result.columns) == ["item_id", "feat"]

    def test_load_parquet_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_parquet("/nonexistent/path/data.parquet")

    def test_load_parquet_with_required_columns(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        pd.DataFrame({"item_id": [1], "name": ["x"]}).to_parquet(path, index=False)

        result = load_parquet(path, required_columns=["item_id", "name"])
        assert len(result) == 1

    def test_load_parquet_missing_required_columns(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        pd.DataFrame({"item_id": [1]}).to_parquet(path, index=False)

        with pytest.raises(ValueError, match="missing required columns.*name"):
            load_parquet(path, required_columns=["item_id", "name"])


class TestUploadParquet:
    def test_upload_and_read_back(self, tmp_path):
        path = str(tmp_path / "output.parquet")
        df = pd.DataFrame({"item_id": ["a"], "emb": [[1.0, 2.0]]})

        upload_parquet(df, path)

        result = pd.read_parquet(path)
        assert len(result) == 1
        assert list(result.columns) == ["item_id", "emb"]

    def test_upload_empty_dataframe(self, tmp_path):
        path = str(tmp_path / "empty.parquet")
        df = pd.DataFrame(columns=["item_id", "emb"])

        upload_parquet(df, path)

        result = pd.read_parquet(path)
        assert len(result) == 0
