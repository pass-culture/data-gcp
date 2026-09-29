"""Unit tests for gcs_utils module."""

from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import pytest
from src.gcs_utils import iter_parquet_chunks, write_embeddings_parquet, write_parquet


class TestIterParquetChunks:
    def _write(self, path, item_ids):
        pd.DataFrame(
            {
                "item_id": list(item_ids),
                "content_hash": [f"h{i}" for i in item_ids],
                "feat": [f"v{i}" for i in item_ids],
            }
        ).to_parquet(path, index=False)

    def test_uniform_chunk_sizing_across_files(self, tmp_path):
        self._write(tmp_path / "a.parquet", range(0, 5))
        self._write(tmp_path / "b.parquet", range(5, 8))
        self._write(tmp_path / "c.parquet", range(8, 20))
        chunks = list(iter_parquet_chunks(str(tmp_path), rows_per_chunk=4))
        assert all(len(c) <= 4 for c in chunks)
        assert sum(len(c) for c in chunks) == 20

    def test_small_file_merged_into_full_chunk(self, tmp_path):
        self._write(tmp_path / "a.parquet", range(0, 5))
        self._write(tmp_path / "b.parquet", range(5, 8))
        self._write(tmp_path / "c.parquet", range(8, 20))
        chunks = list(iter_parquet_chunks(str(tmp_path), rows_per_chunk=4))
        assert [len(c) for c in chunks] == [4, 4, 4, 4, 4]
        assert {4, 5, 6, 7}.issubset(set(chunks[1]["item_id"].tolist()))

    def test_last_chunk_smaller(self, tmp_path):
        self._write(tmp_path / "a.parquet", range(0, 10))
        chunks = list(iter_parquet_chunks(str(tmp_path), rows_per_chunk=4))
        assert [len(c) for c in chunks] == [4, 4, 2]

    def test_row_integrity_preserved(self, tmp_path):
        self._write(tmp_path / "a.parquet", range(0, 5))
        self._write(tmp_path / "b.parquet", range(5, 20))
        all_items = pd.concat(
            list(iter_parquet_chunks(str(tmp_path), rows_per_chunk=4)),
            ignore_index=True,
        )
        assert sorted(all_items["item_id"].tolist()) == list(range(20))
        assert not all_items["item_id"].duplicated().any()

    def test_required_columns_missing_raises(self, tmp_path):
        self._write(tmp_path / "a.parquet", range(0, 3))
        with pytest.raises(ValueError, match="missing required columns.*prompt"):
            list(
                iter_parquet_chunks(
                    str(tmp_path), rows_per_chunk=4, required_columns=["prompt"]
                )
            )

    def test_required_columns_none_skips_check(self, tmp_path):
        self._write(tmp_path / "a.parquet", range(0, 3))
        chunks = list(iter_parquet_chunks(str(tmp_path), rows_per_chunk=4))
        assert len(chunks) == 1

    def test_empty_input_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            list(iter_parquet_chunks(str(tmp_path), rows_per_chunk=4))


class TestWriteParquet:
    def test_round_trips_text_columns(self, tmp_path):
        df = pd.DataFrame(
            {"item_id": ["a"], "content_hash": ["h1"], "prompt": ["hello world"]}
        )
        path = tmp_path / "prompts_0.parquet"
        write_parquet(df, str(path))
        assert pd.read_parquet(str(path)).equals(df)


class TestWriteEmbeddingsParquet:
    def test_embedding_is_list_of_float(self, tmp_path):
        df = pd.DataFrame(
            {
                "item_id": ["a", "b"],
                "content_hash": ["h1", "h2"],
                "embedding": [[1.0, 2.0], [3.0, 4.0]],
                "mlflow_run_id": ["r1", "r1"],
                "embedding_model": ["m", "m"],
                "embedding_date": [date(2026, 9, 25), date(2026, 9, 25)],
            }
        )
        path = tmp_path / "embeddings_0.parquet"
        write_embeddings_parquet(df, str(path))
        arrow_type = pq.read_schema(str(path)).field("embedding").type
        assert str(arrow_type) == "list<element: float>"

    def test_empty_chunk_shares_schema_with_populated(self, tmp_path):
        # embeddings arrive as lists (object dtype); an empty chunk must still
        # write the explicit list<float> schema, not infer a null/float column.
        populated = pd.DataFrame(
            {
                "item_id": ["a"],
                "content_hash": ["h1"],
                "embedding": [[1.0, 2.0]],
                "mlflow_run_id": ["r1"],
                "embedding_model": ["m"],
                "embedding_date": [date(2026, 9, 25)],
            }
        )
        empty = pd.DataFrame(
            {
                "item_id": pd.Series([], dtype=object),
                "content_hash": pd.Series([], dtype=object),
                "embedding": pd.Series([], dtype=object),
                "mlflow_run_id": pd.Series([], dtype=object),
                "embedding_model": pd.Series([], dtype=object),
                "embedding_date": pd.Series([], dtype=object),
            }
        )
        p_path = tmp_path / "p.parquet"
        e_path = tmp_path / "e.parquet"
        write_embeddings_parquet(populated, str(p_path))
        write_embeddings_parquet(empty, str(e_path))
        assert pq.read_schema(str(p_path)) == pq.read_schema(str(e_path))

    def test_round_trips_values(self, tmp_path):
        df = pd.DataFrame(
            {
                "item_id": ["a"],
                "content_hash": ["h1"],
                "embedding": [[1.0, 2.0]],
                "mlflow_run_id": ["r1"],
                "embedding_model": ["m"],
                "embedding_date": [date(2026, 9, 25)],
            }
        )
        path = tmp_path / "chunk.parquet"
        write_embeddings_parquet(df, str(path))
        result = pd.read_parquet(str(path)).set_index("item_id")
        assert result.loc["a", "embedding"].tolist() == [1.0, 2.0]
        assert result.loc["a", "mlflow_run_id"] == "r1"
        assert result.loc["a", "embedding_model"] == "m"
        assert result.loc["a", "embedding_date"] == date(2026, 9, 25)
