"""Unit tests for gcs_utils module."""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from gcs_utils import load_parquet, upload_parquet


class TestLoadParquet:
    def test_load_valid_parquet(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        df = pd.DataFrame({"item_id": ["a", "b"], "feat": [1, 2]})
        df.to_parquet(path, index=False)

        result = load_parquet(path)
        assert len(result) == 2
        assert set(result.columns) == {"item_id", "feat"}

    def test_load_parquet_file_not_found(self):
        with pytest.raises((FileNotFoundError, OSError)):
            load_parquet("/nonexistent/path/data.parquet")

    def test_load_parquet_with_column_pruning(self, tmp_path):
        """Only requested columns are read."""
        path = str(tmp_path / "data.parquet")
        pd.DataFrame({"item_id": [1], "name": ["x"], "extra": [99]}).to_parquet(
            path, index=False
        )

        result = load_parquet(path, columns=["item_id", "name"])
        assert list(result.columns) == ["item_id", "name"]
        assert "extra" not in result.columns

    def test_load_parquet_missing_columns(self, tmp_path):
        path = str(tmp_path / "data.parquet")
        pd.DataFrame({"item_id": [1]}).to_parquet(path, index=False)

        with pytest.raises(ValueError, match="missing required columns.*name"):
            load_parquet(path, columns=["item_id", "name"])

    def test_load_parquet_from_directory(self, tmp_path):
        """Sharded parquet files in a directory are read as one dataset."""
        for i in range(3):
            shard = tmp_path / f"shard_{i}.parquet"
            pd.DataFrame({"item_id": [f"item_{i}"], "feat": [i]}).to_parquet(
                str(shard), index=False
            )

        result = load_parquet(str(tmp_path))
        assert len(result) == 3
        assert set(result.columns) == {"item_id", "feat"}

    def test_load_parquet_directory_with_column_pruning(self, tmp_path):
        """Column pruning also works when reading a directory."""
        for i in range(2):
            shard = tmp_path / f"shard_{i}.parquet"
            pd.DataFrame(
                {"item_id": [f"id_{i}"], "used": [1], "unused": [2]}
            ).to_parquet(str(shard), index=False)

        result = load_parquet(str(tmp_path), columns=["item_id", "used"])
        assert len(result) == 2
        assert "unused" not in result.columns

    def test_load_parquet_glob_pattern(self, tmp_path):
        """Glob patterns resolve to matching files."""
        # Create matching and non-matching files
        for i in range(3):
            pd.DataFrame({"item_id": [f"id_{i}"]}).to_parquet(
                str(tmp_path / f"data_{i}.parquet"), index=False
            )
        pd.DataFrame({"item_id": ["other"]}).to_parquet(
            str(tmp_path / "other.parquet"), index=False
        )

        result = load_parquet(str(tmp_path / "data_*.parquet"))
        assert len(result) == 3

    def test_load_parquet_glob_no_match(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No files matching"):
            load_parquet(str(tmp_path / "nonexistent_*.parquet"))


class TestUploadParquet:
    def test_upload_and_read_back(self, tmp_path):
        path = str(tmp_path / "output.parquet")
        df = pd.DataFrame({"item_id": ["a"], "emb": [[1.0, 2.0]]})

        upload_parquet(df, path)

        result = pd.read_parquet(path)
        assert len(result) == 1
        assert set(result.columns) == {"item_id", "emb"}

    def test_upload_empty_dataframe(self, tmp_path):
        path = str(tmp_path / "empty.parquet")
        df = pd.DataFrame(columns=["item_id", "emb"])

        upload_parquet(df, path)

        result = pd.read_parquet(path)
        assert len(result) == 0

    def test_upload_respects_row_group_size(self, tmp_path):
        """Custom row_group_size is reflected in the written file."""
        path = str(tmp_path / "rg.parquet")
        df = pd.DataFrame({"x": range(1000)})

        upload_parquet(df, path, row_group_size=300)

        pf = pq.ParquetFile(path)
        assert pf.metadata.num_row_groups == 4  # ceil(1000/300)

    def test_upload_arrow_backed_columns(self, tmp_path):
        """DataFrames with Arrow-backed list columns are written correctly."""
        path = str(tmp_path / "arrow.parquet")

        # Build an Arrow-backed embedding column (same as embedding.py does)
        flat = pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
        arrow_list = pa.FixedSizeListArray.from_arrays(flat, list_size=2)
        emb_col = pd.arrays.ArrowExtensionArray(pa.chunked_array([arrow_list]))

        df = pd.DataFrame({"item_id": ["a", "b", "c"], "emb": emb_col})
        upload_parquet(df, path)

        # Read back via PyArrow to avoid pandas ArrowDtype parsing issues
        table = pq.read_table(path)
        assert table.num_rows == 3
        assert table.column("emb")[0].as_py() == [1.0, 2.0]
        assert table.column("emb")[2].as_py() == [5.0, 6.0]
