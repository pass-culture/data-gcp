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
