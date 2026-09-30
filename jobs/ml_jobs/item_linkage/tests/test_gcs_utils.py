import pytest

from utils.gcs_utils import _parse_gcs_path


class TestParseGcsPath:
    def test_splits_bucket_and_blob(self):
        bucket, blob = _parse_gcs_path("gs://my-bucket/path/to/file.parquet")
        assert bucket == "my-bucket"
        assert blob == "path/to/file.parquet"

    def test_bucket_only_path(self):
        bucket, blob = _parse_gcs_path("gs://my-bucket")
        assert bucket == "my-bucket"
        assert blob == ""

    def test_non_gcs_path_raises(self):
        with pytest.raises(ValueError, match="gs://"):
            _parse_gcs_path("s3://my-bucket/file")

    def test_plain_path_raises(self):
        with pytest.raises(ValueError, match="gs://"):
            _parse_gcs_path("/local/path/file.parquet")
