import pytest
from utils.gcs_utils import _parse_gcs_path


def test_parse_gcs_path():
    # Test case 1: Normal case
    bucket, blob = _parse_gcs_path("gs://my-bucket/my-blob")
    assert bucket == "my-bucket"
    assert blob == "my-blob"

    # Test case 2: Path without blob
    bucket, blob = _parse_gcs_path("gs://my-bucket/")
    assert bucket == "my-bucket"
    assert blob == ""

    # Test case 3: Invalid path
    with pytest.raises(ValueError):
        _parse_gcs_path("invalid-path")

    # Test case 4: Path without gs://
    with pytest.raises(ValueError):
        _parse_gcs_path("my-bucket/my-blob")
