import mimetypes
import uuid
from pathlib import PurePosixPath
from urllib.parse import urlparse


def poster_uuid(poster_url: str) -> str:
    """Return a deterministic UUID5 for a given poster URL."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, poster_url))


def detect_extension(url: str, content_type: str | None) -> str:
    """Derive a file extension from the URL path, falling back to Content-Type, then 'jpg'."""
    suffix = PurePosixPath(urlparse(url).path).suffix
    if suffix:
        return suffix.lstrip(".")
    if content_type:
        ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if ext:
            return ext.lstrip(".")
    return "jpg"
