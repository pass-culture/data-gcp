from unittest.mock import MagicMock, patch

import pytest

from src.common.gcp import get_dates_from_bucket, get_last_date_from_bucket

GCS_PATH = "gs://data-bucket-prod/dump_wikidata"
FILE_NAME = "wikidata_extraction.parquet"


class FakeBlobIterator:
    """Mimic google.api_core HTTPIterator: `prefixes` is filled while iterating."""

    def __init__(self, prefixes: list[str]):
        self._prefixes = prefixes
        self.prefixes = set()

    def __iter__(self):
        self.prefixes = set(self._prefixes)
        return iter([])


def _mock_client(prefixes: list[str], existing_files: set[str]) -> MagicMock:
    client = MagicMock()
    client.list_blobs.side_effect = lambda *args, **kwargs: FakeBlobIterator(prefixes)

    def _blob(name):
        blob = MagicMock()
        blob.exists.return_value = name in existing_files
        return blob

    client.bucket.return_value.blob.side_effect = _blob
    return client


@patch("src.common.gcp.storage.Client")
def test_get_dates_from_bucket_ignores_nested_and_non_date_folders(mock_client_cls):
    mock_client_cls.return_value = _mock_client(
        prefixes=[
            "dump_wikidata/20261001/",
            "dump_wikidata/20261101/",
            "dump_wikidata/tmp/",
            "dump_wikidata/20250519/",
        ],
        existing_files=set(),
    )

    assert get_dates_from_bucket(GCS_PATH) == ["20261101", "20261001", "20250519"]
    mock_client_cls.return_value.list_blobs.assert_called_once_with(
        "data-bucket-prod", prefix="dump_wikidata/", delimiter="/"
    )


@patch("src.common.gcp.storage.Client")
def test_get_last_date_from_bucket_returns_latest_complete_folder(mock_client_cls):
    mock_client_cls.return_value = _mock_client(
        prefixes=["dump_wikidata/20261001/", "dump_wikidata/20261101/"],
        existing_files={
            f"dump_wikidata/20261001/{FILE_NAME}",
            f"dump_wikidata/20261101/{FILE_NAME}",
        },
    )

    assert get_last_date_from_bucket(GCS_PATH, FILE_NAME) == "20261101"


@patch("src.common.gcp.storage.Client")
def test_get_last_date_from_bucket_skips_folder_missing_required_file(
    mock_client_cls,
):
    # 20261101 only holds raw/ files: the merge never produced the final file
    mock_client_cls.return_value = _mock_client(
        prefixes=["dump_wikidata/20261001/", "dump_wikidata/20261101/"],
        existing_files={f"dump_wikidata/20261001/{FILE_NAME}"},
    )

    assert get_last_date_from_bucket(GCS_PATH, FILE_NAME) == "20261001"


@patch("src.common.gcp.storage.Client")
def test_get_last_date_from_bucket_raises_when_no_folder_has_required_file(
    mock_client_cls,
):
    mock_client_cls.return_value = _mock_client(
        prefixes=["dump_wikidata/20261101/"], existing_files=set()
    )

    with pytest.raises(ValueError, match="No date folder"):
        get_last_date_from_bucket(GCS_PATH, FILE_NAME)


@patch("src.common.gcp.storage.Client")
def test_get_last_date_from_bucket_raises_when_no_dates(mock_client_cls):
    mock_client_cls.return_value = _mock_client(prefixes=[], existing_files=set())

    with pytest.raises(ValueError, match="No dates found"):
        get_last_date_from_bucket(GCS_PATH, FILE_NAME)
