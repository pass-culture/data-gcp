import importlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import sqlglot
from airflow.exceptions import AirflowException
from sqlglot import exp


@pytest.fixture
def adjust_utils():
    return importlib.import_module("dependencies.adjust.utils")


@pytest.fixture
def bigquery_client(adjust_utils, monkeypatch):
    client = Mock()
    monkeypatch.setattr(adjust_utils, "get_bigquery_client", lambda **kwargs: client)
    return client


def test_prepare_tables_discovers_and_checks_schema(adjust_utils, bigquery_client):
    table_names = ["2026-10-06_app_store_Organic"]
    bigquery_client.query.return_value.result.return_value = [
        SimpleNamespace(table_name=table_name) for table_name in table_names
    ]
    bigquery_client.get_table.return_value.schema = [
        SimpleNamespace(name=name, field_type=data_type, mode="NULLABLE")
        for name, data_type in adjust_utils.EXPECTED_SCHEMA.items()
    ]

    result = adjust_utils.discover_and_validate_adjust_tables("2026-10-06")

    assert result == table_names
    assert bigquery_client.get_table.call_args.args[0] == (
        "passculture-data-ehp.adjust_import_dev.2026-10-06_app_store_Organic"
    )
    assert "INFORMATION_SCHEMA.TABLES" in bigquery_client.query.call_args.args[0]


def test_prepare_rejects_incompatible_schema(adjust_utils, bigquery_client):
    bigquery_client.query.return_value.result.return_value = [
        SimpleNamespace(table_name="2026-10-06_example")
    ]
    bigquery_client.get_table.return_value.schema = []

    with pytest.raises(AirflowException, match="Invalid Adjust schema"):
        adjust_utils.discover_and_validate_adjust_tables("2026-10-06")


def test_prepare_rejects_no_tables(adjust_utils, bigquery_client):
    bigquery_client.query.return_value.result.return_value = []

    with pytest.raises(AirflowException, match="No Adjust tables found"):
        adjust_utils.discover_and_validate_adjust_tables("2026-10-06")


def test_adjust_import_is_dev_only(adjust_utils, monkeypatch):
    monkeypatch.setenv("ENV_SHORT_NAME", "prod")

    with pytest.raises(AirflowException, match="restricted to dev"):
        adjust_utils.get_bigquery_client()


def test_staging_builds_union_with_explicit_columns(
    adjust_utils, bigquery_client, monkeypatch
):
    bigquery_client.get_table.return_value.num_rows = 2
    table_names = ["2026-10-06_first", "2026-10-06_second"]
    validate_staging = Mock()
    monkeypatch.setattr(adjust_utils, "validate_adjust_staging", validate_staging)

    result = adjust_utils.stage_and_validate_adjust_tables(
        table_names, "2026-10-06", "import_adjust__test__user", "manual__test"
    )

    query = bigquery_client.query.call_args.args[0]
    parsed = sqlglot.parse_one(query, read="bigquery")
    assert isinstance(parsed, exp.Create)
    assert len(list(parsed.find_all(exp.Select))) == 2
    assert all(len(select.expressions) == 20 for select in parsed.find_all(exp.Select))
    assert not list(parsed.find_all(exp.Star))
    assert "DATE(DAY) AS DAY" in query.upper()
    assert "INTERVAL 24 HOUR" in query.upper()
    assert result["row_count"] == 2
    assert (
        "passculture-data-ehp.adjust_import_dev.adjust_staging_20261006_"
        in result["table_id"]
    )
    validate_staging.assert_called_once_with(result, table_names, "2026-10-06")


@pytest.mark.parametrize(
    "summary",
    [
        SimpleNamespace(row_count=1, invalid_day_rows=1, invalid_source_rows=0),
        SimpleNamespace(row_count=1, invalid_day_rows=0, invalid_source_rows=1),
    ],
)
def test_staging_validation_rejects_bad_data(adjust_utils, bigquery_client, summary):
    bigquery_client.query.return_value.result.return_value = [summary]
    staging_result = {
        "table_id": "passculture-data-ehp.adjust_import_dev.adjust_staging_test",
        "row_count": 1,
    }

    with pytest.raises(AirflowException, match="invalid dates or unknown source"):
        adjust_utils.validate_adjust_staging(
            staging_result, ["2026-10-06_example"], "2026-10-06"
        )


def test_staging_validation_checks_staging_only(adjust_utils, bigquery_client):
    bigquery_client.query.return_value.result.return_value = [
        SimpleNamespace(row_count=1, invalid_day_rows=0, invalid_source_rows=0)
    ]
    staging_result = {
        "table_id": "passculture-data-ehp.adjust_import_dev.adjust_staging_test",
        "row_count": 1,
    }

    result = adjust_utils.validate_adjust_staging(
        staging_result, ["2026-10-06_example"], "2026-10-06"
    )

    assert result == staging_result
    query_call = bigquery_client.query.call_args
    query = query_call.args[0]
    assert "UNNEST(@SOURCE_TABLE_NAMES)" in query.upper()
    assert "FROM `PASSCULTURE-DATA-PROD.ADJUST_IMPORT_PROD" not in query.upper()
    assert query_call.kwargs["job_config"].query_parameters[1].values == [
        "2026-10-06_example"
    ]


def test_staging_validation_rejects_changed_total(adjust_utils, bigquery_client):
    bigquery_client.query.return_value.result.return_value = [
        SimpleNamespace(row_count=2, invalid_day_rows=0, invalid_source_rows=0)
    ]
    staging_result = {
        "table_id": "passculture-data-ehp.adjust_import_dev.adjust_staging_test",
        "row_count": 1,
    }

    with pytest.raises(AirflowException, match="row count changed"):
        adjust_utils.validate_adjust_staging(
            staging_result, ["2026-10-06_example"], "2026-10-06"
        )


def test_copy_moves_staging_from_eu_to_dev(adjust_utils, bigquery_client):
    bigquery_client.get_table.return_value = SimpleNamespace(
        num_rows=1, location="europe-west1", expires="tomorrow"
    )
    staging_result = {
        "table_id": "passculture-data-ehp.adjust_import_dev.adjust_staging_test",
        "row_count": 1,
    }

    result = adjust_utils.copy_adjust_staging(staging_result)

    assert result["table_id"] == "passculture-data-ehp.tmp_dev.adjust_staging_test"
    assert result["row_count"] == 1
    assert bigquery_client.copy_table.call_args.kwargs["location"] == "EU"
    assert (
        bigquery_client.copy_table.call_args.kwargs["job_config"].write_disposition
        == "WRITE_TRUNCATE"
    )


def test_copy_rejects_unexpected_row_count(adjust_utils, bigquery_client):
    bigquery_client.get_table.return_value = SimpleNamespace(
        num_rows=2, location="europe-west1"
    )
    staging_result = {
        "table_id": "passculture-data-ehp.adjust_import_dev.adjust_staging_test",
        "row_count": 1,
    }

    with pytest.raises(AirflowException, match="row count mismatch"):
        adjust_utils.copy_adjust_staging(staging_result)


def test_publish_replaces_only_requested_partition(adjust_utils, bigquery_client):
    copy_result = {
        "table_id": "passculture-data-ehp.tmp_dev.adjust_staging_test",
        "row_count": 2,
    }

    result = adjust_utils.publish_adjust_partition(
        copy_result, "2026-10-06", "import_adjust__test__user"
    )

    assert result["row_count"] == 2
    assert result["table_id"].startswith(
        "passculture-data-ehp.raw_dev.adjust_daily_test_"
    )
    query_call = bigquery_client.query.call_args
    assert query_call.kwargs["job_config"].destination.table_id.endswith("$20261006")
    assert query_call.kwargs["job_config"].write_disposition == "WRITE_TRUNCATE"
    assert query_call.kwargs["job_config"].time_partitioning.field == "day"
    assert bigquery_client.query.call_count == 1


def test_publish_rejects_empty_day(adjust_utils, bigquery_client):
    with pytest.raises(AirflowException, match="Refusing to publish an empty"):
        adjust_utils.publish_adjust_partition(
            {
                "table_id": "passculture-data-ehp.tmp_dev.adjust_staging_test",
                "row_count": 0,
            },
            "2026-10-06",
            "test_dag",
        )

    bigquery_client.query.assert_not_called()


@pytest.mark.parametrize(
    "template_name",
    [
        "discover_tables.sql",
        "stage_tables.sql",
        "validate_staging.sql",
        "publish_partition.sql",
    ],
)
def test_sql_templates_render_and_parse(adjust_utils, template_name):
    query = adjust_utils.render_sql(
        template_name,
        source_project="passculture-data-ehp",
        source_dataset="adjust_import_dev",
        staging_table_id="passculture-data-ehp.adjust_import_dev.adjust_staging_test",
        table_names=["2026-10-06_first", "2026-10-06_second"],
        table_id="passculture-data-ehp.tmp_dev.adjust_staging_test",
    )

    parsed = sqlglot.parse_one(query, read="bigquery")
    assert parsed is not None
    assert "{{" not in query and "{%" not in query


def test_dag_has_only_dev_source_and_simple_task_chain(adjust_utils):
    dag = importlib.import_module("jobs.import.import_adjust").dag

    assert set(dag.params) == {"export_date"}
    assert set(dag.task_ids) == {
        "start",
        "prepare_tables",
        "stage_and_validate",
        "copy_to_tmp",
        "publish_partition",
        "end",
    }
    assert dag.get_task("start").downstream_task_ids == {"prepare_tables"}
    assert dag.get_task("prepare_tables").python_callable is (
        adjust_utils.discover_and_validate_adjust_tables
    )
    assert dag.get_task("stage_and_validate").downstream_task_ids == {"copy_to_tmp"}
    assert dag.schedule_interval is None
    assert dag.max_active_runs == 1
