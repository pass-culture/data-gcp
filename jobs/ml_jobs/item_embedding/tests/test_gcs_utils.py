"""Unit tests for gcs_utils module."""

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from config import Vector
from gcs_utils import (
    _validate_parquet_file,
    iter_metadata_chunks,
    write_embeddings_parquet,
)


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


class TestWriteEmbeddingsParquet:
    def _vectors(self):
        return [Vector(name="movies_content", features=["title"], encoder_name="model")]

    def test_populated_column_is_list_of_double(self, tmp_path):
        df = pd.DataFrame(
            {
                "item_id": ["a", "b"],
                "content_hash": ["h1", "h2"],
                "movies_content": [[1.0, 2.0], [3.0, 4.0]],
            }
        )
        path = tmp_path / "chunk_0.parquet"

        write_embeddings_parquet(df, self._vectors(), str(path))

        arrow_type = pq.read_schema(str(path)).field("movies_content").type
        assert str(arrow_type) == "list<element: double>"

    def test_all_null_column_is_still_list_of_double_not_null_type(self, tmp_path):
        # A chunk where the scoped vector matches zero items (e.g. a chunk
        # with no movies) has no non-null value to infer the list's element
        # type from; without an explicit schema pyarrow would fall back to
        # Arrow's `null` type here instead of `list<double>`.
        df = pd.DataFrame(
            {
                "item_id": ["a", "b"],
                "content_hash": ["h1", "h2"],
                "movies_content": pd.Series([np.nan, np.nan], dtype=object),
            }
        )
        path = tmp_path / "chunk_1.parquet"

        write_embeddings_parquet(df, self._vectors(), str(path))

        arrow_type = pq.read_schema(str(path)).field("movies_content").type
        assert str(arrow_type) == "list<element: double>"

    def test_populated_and_empty_chunks_share_identical_schema(self, tmp_path):
        # This is what actually fixes GCSToBigQueryOperator's autodetect:
        # every chunk file must agree on the column's Arrow type, or
        # BigQuery can't unify them into a single REPEATED FLOAT field.
        populated = pd.DataFrame(
            {
                "item_id": ["a"],
                "content_hash": ["h1"],
                "movies_content": [[1.0, 2.0]],
            }
        )
        empty = pd.DataFrame(
            {
                "item_id": ["b"],
                "content_hash": ["h2"],
                "movies_content": pd.Series([np.nan], dtype=object),
            }
        )
        populated_path = tmp_path / "chunk_populated.parquet"
        empty_path = tmp_path / "chunk_empty.parquet"

        write_embeddings_parquet(populated, self._vectors(), str(populated_path))
        write_embeddings_parquet(empty, self._vectors(), str(empty_path))

        assert pq.read_schema(str(populated_path)) == pq.read_schema(str(empty_path))

    def test_round_trips_values_and_missing_rows(self, tmp_path):
        df = pd.DataFrame(
            {
                "item_id": ["a", "b"],
                "content_hash": ["h1", "h2"],
                "movies_content": [[1.0, 2.0], np.nan],
            }
        )
        path = tmp_path / "chunk.parquet"

        write_embeddings_parquet(df, self._vectors(), str(path))

        result = pd.read_parquet(str(path))
        assert result.set_index("item_id").loc["a", "movies_content"].tolist() == [
            1.0,
            2.0,
        ]
        assert result.set_index("item_id").loc["b", "movies_content"] is None
