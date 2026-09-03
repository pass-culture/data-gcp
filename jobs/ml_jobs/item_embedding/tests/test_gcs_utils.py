"""Unit tests for gcs_utils module."""

import pandas as pd
import pytest
from config import Vector
from gcs_utils import _validate_parquet_file, iter_metadata_chunks


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


class TestIterMetadataChunks:
    def _write_parquet(self, path, item_ids):
        df = pd.DataFrame(
            {
                "item_id": item_ids,
                "content_hash": [f"h{i}" for i in item_ids],
                "feat": [f"v{i}" for i in item_ids],
            }
        )
        df.to_parquet(path, index=False)

    def _vectors(self):
        return [Vector(name="v", features=["feat"], encoder_name="model")]

    def test_uniform_chunk_sizing_across_multiple_files(self, tmp_path):
        # Uneven input files (5, 3, 12 rows) must be re-chunked into
        # uniform pieces capped at rows_per_chunk.
        self._write_parquet(tmp_path / "a.parquet", range(0, 5))
        self._write_parquet(tmp_path / "b.parquet", range(5, 8))
        self._write_parquet(tmp_path / "c.parquet", range(8, 20))

        chunks = list(
            iter_metadata_chunks(str(tmp_path), self._vectors(), rows_per_chunk=4)
        )

        assert all(len(chunk) <= 4 for chunk in chunks)
        assert sum(len(chunk) for chunk in chunks) == 20

    def test_small_file_is_merged_into_a_full_chunk(self, tmp_path):
        # pyarrow's to_batches() caps a batch at rows_per_chunk but never
        # merges across file/fragment boundaries on its own -- a small file
        # would otherwise always produce its own short trailing batch. This
        # is the behavior iter_metadata_chunks must fix: the 5-row file's
        # last row (item_id=4) and the whole 3-row file (item_ids 5,6,7)
        # must be combined into one full 4-row chunk, not left as two
        # separate short ones.
        self._write_parquet(tmp_path / "a.parquet", range(0, 5))
        self._write_parquet(tmp_path / "b.parquet", range(5, 8))
        self._write_parquet(tmp_path / "c.parquet", range(8, 20))

        chunks = list(
            iter_metadata_chunks(str(tmp_path), self._vectors(), rows_per_chunk=4)
        )

        assert [len(chunk) for chunk in chunks] == [4, 4, 4, 4, 4]
        merged_chunk_ids = set(chunks[1]["item_id"].tolist())
        assert {4, 5, 6, 7}.issubset(merged_chunk_ids)

    def test_last_chunk_smaller(self, tmp_path):
        self._write_parquet(tmp_path / "a.parquet", range(0, 10))

        chunks = list(
            iter_metadata_chunks(str(tmp_path), self._vectors(), rows_per_chunk=4)
        )

        assert [len(chunk) for chunk in chunks] == [4, 4, 2]

    def test_row_integrity_preserved_across_chunk_boundaries(self, tmp_path):
        self._write_parquet(tmp_path / "a.parquet", range(0, 5))
        self._write_parquet(tmp_path / "b.parquet", range(5, 8))
        self._write_parquet(tmp_path / "c.parquet", range(8, 20))

        chunks = list(
            iter_metadata_chunks(str(tmp_path), self._vectors(), rows_per_chunk=4)
        )
        all_items = pd.concat(chunks, ignore_index=True)

        assert sorted(all_items["item_id"].tolist()) == list(range(20))
        assert not all_items["item_id"].duplicated().any()

    def test_column_validation_failure_raises(self, tmp_path):
        self._write_parquet(tmp_path / "a.parquet", range(0, 3))
        vectors = [Vector(name="v", features=["missing_feat"], encoder_name="model")]

        with pytest.raises(ValueError, match="missing required columns.*missing_feat"):
            list(iter_metadata_chunks(str(tmp_path), vectors, rows_per_chunk=4))

    def test_empty_input_raises_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            list(iter_metadata_chunks(str(tmp_path), self._vectors(), rows_per_chunk=4))
