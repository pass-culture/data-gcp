from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa
import pytest
from google.cloud import bigquery

from src.common import bigquery as bq_module


def test_read_dataframe_returns_polars_dataframe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = MagicMock()
    result.to_arrow.return_value = pa.table({"item_id": ["1", "2"]})
    query_job = MagicMock()
    query_job.result.return_value = result
    client = MagicMock()
    client.query.return_value = query_job
    monkeypatch.setattr(bq_module.bigquery, "Client", lambda project=None: client)

    df = bq_module.read_dataframe("SELECT * FROM t")

    assert isinstance(df, pl.DataFrame)
    assert df["item_id"].to_list() == ["1", "2"]
    client.query.assert_called_once()


def test_read_dataframe_passes_query_parameters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = MagicMock()
    result.to_arrow.return_value = pa.table({"item_id": []})
    query_job = MagicMock()
    query_job.result.return_value = result
    client = MagicMock()
    client.query.return_value = query_job
    monkeypatch.setattr(bq_module.bigquery, "Client", lambda project=None: client)

    params = [bigquery.ScalarQueryParameter("dim", "INT64", 256)]
    bq_module.read_dataframe("SELECT @dim", params)

    _, kwargs = client.query.call_args
    assert kwargs["job_config"].query_parameters == params


def test_write_dataframe_loads_parquet_and_waits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = MagicMock()
    client = MagicMock()
    client.load_table_from_file.return_value = job
    monkeypatch.setattr(bq_module.bigquery, "Client", lambda project=None: client)

    data = pl.DataFrame({"item_id": ["1", "2"]})
    bq_module.write_dataframe(data, dataset="ml_feat_dev", table="topic_item_document")

    client.load_table_from_file.assert_called_once()
    _, kwargs = client.load_table_from_file.call_args
    assert kwargs["destination"].endswith("ml_feat_dev.topic_item_document")
    job.result.assert_called_once()


def test_write_dataframe_passes_explicit_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job = MagicMock()
    client = MagicMock()
    client.load_table_from_file.return_value = job
    monkeypatch.setattr(bq_module.bigquery, "Client", lambda project=None: client)

    schema = [bigquery.SchemaField("item_id", "STRING")]
    data = pl.DataFrame({"item_id": ["1"]})
    bq_module.write_dataframe(data, dataset="ml_feat_dev", table="t", schema=schema)

    _, kwargs = client.load_table_from_file.call_args
    job_config = kwargs["job_config"]
    assert job_config.schema == schema
    assert job_config.autodetect is False


def test_merge_upsert_creates_table_stages_and_runs_merge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    query_job = MagicMock()
    client.query.return_value = query_job
    monkeypatch.setattr(bq_module.bigquery, "Client", lambda project=None: client)
    write_dataframe_mock = MagicMock()
    monkeypatch.setattr(bq_module, "write_dataframe", write_dataframe_mock)

    schema = [
        bigquery.SchemaField("item_id", "STRING"),
        bigquery.SchemaField("value", "STRING"),
    ]
    data = pl.DataFrame({"item_id": ["1"], "value": ["a"]})

    bq_module.merge_upsert(
        data,
        "ml_feat_dev",
        "topic_item_document",
        key_columns=["item_id"],
        schema=schema,
    )

    client.create_table.assert_called_once()
    write_dataframe_mock.assert_called_once()
    _, write_kwargs = write_dataframe_mock.call_args
    assert write_kwargs["write_disposition"] == bigquery.WriteDisposition.WRITE_TRUNCATE

    merge_sql = client.query.call_args[0][0]
    assert "MERGE" in merge_sql
    assert "T.item_id = S.item_id" in merge_sql
    assert "WHEN MATCHED THEN UPDATE SET value = S.value" in merge_sql
    assert (
        "WHEN NOT MATCHED THEN INSERT (item_id, value) VALUES (S.item_id, S.value)"
        in merge_sql
    )
    query_job.result.assert_called_once()
    client.delete_table.assert_called_once()
    assert client.delete_table.call_args.kwargs["not_found_ok"] is True


def test_merge_upsert_drops_staging_table_even_if_merge_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = MagicMock()
    client.query.side_effect = RuntimeError("boom")
    monkeypatch.setattr(bq_module.bigquery, "Client", lambda project=None: client)
    monkeypatch.setattr(bq_module, "write_dataframe", MagicMock())

    schema = [bigquery.SchemaField("item_id", "STRING")]
    data = pl.DataFrame({"item_id": ["1"]})

    with pytest.raises(RuntimeError, match="boom"):
        bq_module.merge_upsert(
            data, "ml_feat_dev", "t", key_columns=["item_id"], schema=schema
        )

    client.delete_table.assert_called_once()
