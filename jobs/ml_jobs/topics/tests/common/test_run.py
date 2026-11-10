import pathlib

from src.common.run import new_run_id, read_run_id, write_run_id


def test_new_run_id_is_unique() -> None:
    assert new_run_id() != new_run_id()


def test_write_then_read_run_id_roundtrip(tmp_path: pathlib.Path) -> None:
    run_id_file = str(tmp_path / "mlflow_run_id.txt")
    write_run_id("abc123", run_id_file)
    assert read_run_id(run_id_file) == "abc123"


def test_read_run_id_missing_file_returns_empty_string(tmp_path: pathlib.Path) -> None:
    missing_file = str(tmp_path / "does_not_exist.txt")
    assert read_run_id(missing_file) == ""
