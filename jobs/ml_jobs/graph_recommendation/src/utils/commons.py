BUCKET_PREFIX = "gs://"


def is_bucket_path(path: str) -> bool:
    """Check if a given path is a cloud storage bucket path."""
    return path.startswith(BUCKET_PREFIX)
