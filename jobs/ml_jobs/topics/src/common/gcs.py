"""GCS paths for intermediate files and versioned models (spec section 4.2),
plus the "latest" pointer that marks which model version is in use (sections
7.2/7.3).
"""

from google.cloud import storage

from src.common.constants import ENV_SHORT_NAME, ML_BUCKET, ML_BUCKET_TEMP

LATEST_POINTER_BLOB = "latest"


def _split_gcs_path(gcs_path: str) -> tuple[str, str]:
    """``"gs://bucket/a/b"`` -> ``("bucket", "a/b")``."""
    without_scheme = gcs_path.removeprefix("gs://")
    bucket_name, _, blob_path = without_scheme.partition("/")
    return bucket_name, blob_path


def temp_path(run_date: str, *parts: str) -> str:
    """``gs://{ML_BUCKET_TEMP}/topics_{env}/{run_date}/{parts...}``."""
    joined = "/".join(parts)
    base = f"gs://{ML_BUCKET_TEMP}/topics_{ENV_SHORT_NAME}/{run_date}"
    return f"{base}/{joined}" if joined else base


def model_path(split_level: int, corpus_id: str, run_id: str) -> str:
    """``gs://{ML_BUCKET}/topics_{env}/models/level{split_level}/{corpus_id}/{run_id}/``."""
    return (
        f"gs://{ML_BUCKET}/topics_{ENV_SHORT_NAME}/models/"
        f"level{split_level}/{corpus_id}/{run_id}/"
    )


def latest_pointer_path(split_level: int, corpus_id: str) -> str:
    """The pointer file recording which ``run_id``'s model is currently in use."""
    return (
        f"gs://{ML_BUCKET}/topics_{ENV_SHORT_NAME}/models/"
        f"level{split_level}/{corpus_id}/{LATEST_POINTER_BLOB}"
    )


def read_latest_run_id(split_level: int, corpus_id: str) -> str | None:
    """The ``run_id`` of the model currently marked "latest" for a corpus, or
    ``None`` if no model has been published yet.
    """
    bucket_name, blob_path = _split_gcs_path(
        latest_pointer_path(split_level, corpus_id)
    )
    blob = storage.Client().bucket(bucket_name).blob(blob_path)
    if not blob.exists():
        return None
    return blob.download_as_text().strip()


def write_latest_pointer(split_level: int, corpus_id: str, run_id: str) -> None:
    """Mark ``run_id`` as the model version to use for a corpus."""
    bucket_name, blob_path = _split_gcs_path(
        latest_pointer_path(split_level, corpus_id)
    )
    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_string(run_id)
