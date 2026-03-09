"""Unit tests for gcs_utils module."""

import pandas as pd
import pytest
from config import Vector
from gcs_utils import _validate_parquet_file


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
