from unittest.mock import MagicMock

import pytest

from src.common import gcs


def test_temp_path_without_extra_parts() -> None:
    path = gcs.temp_path("20261110")
    assert path == f"gs://{gcs.ML_BUCKET_TEMP}/topics_{gcs.ENV_SHORT_NAME}/20261110"


def test_temp_path_joins_extra_parts() -> None:
    path = gcs.temp_path("20261110", "corpus_01000000", "embeddings.parquet")
    assert path == (
        f"gs://{gcs.ML_BUCKET_TEMP}/topics_{gcs.ENV_SHORT_NAME}/20261110/"
        "corpus_01000000/embeddings.parquet"
    )


def test_model_path() -> None:
    path = gcs.model_path(split_level=1, corpus_id="01000000", run_id="abc123")
    assert path == (
        f"gs://{gcs.ML_BUCKET}/topics_{gcs.ENV_SHORT_NAME}/models/"
        "level1/01000000/abc123/"
    )


def test_latest_pointer_path() -> None:
    path = gcs.latest_pointer_path(split_level=1, corpus_id="01000000")
    assert path == (
        f"gs://{gcs.ML_BUCKET}/topics_{gcs.ENV_SHORT_NAME}/models/"
        "level1/01000000/latest"
    )


def test_split_gcs_path() -> None:
    assert gcs._split_gcs_path("gs://bucket-name/a/b/c") == ("bucket-name", "a/b/c")


def test_read_latest_run_id_missing_blob_returns_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blob = MagicMock()
    blob.exists.return_value = False
    client = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    monkeypatch.setattr(gcs.storage, "Client", lambda: client)

    assert gcs.read_latest_run_id(split_level=1, corpus_id="01000000") is None


def test_read_latest_run_id_returns_stripped_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    blob = MagicMock()
    blob.exists.return_value = True
    blob.download_as_text.return_value = "abc123\n"
    client = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    monkeypatch.setattr(gcs.storage, "Client", lambda: client)

    assert gcs.read_latest_run_id(split_level=1, corpus_id="01000000") == "abc123"


def test_write_latest_pointer_uploads_run_id(monkeypatch: pytest.MonkeyPatch) -> None:
    blob = MagicMock()
    client = MagicMock()
    client.bucket.return_value.blob.return_value = blob
    monkeypatch.setattr(gcs.storage, "Client", lambda: client)

    gcs.write_latest_pointer(split_level=1, corpus_id="01000000", run_id="abc123")

    blob.upload_from_string.assert_called_once_with("abc123")
