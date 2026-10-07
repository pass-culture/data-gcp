"""Startup helper that materialises the semantic LanceDB on local disk.

At container startup the `SemanticClient` downloads that GCS directory to local disk once,
then opens it locally. Vector search is run against fast local storage instead of over the network.

The configured `SEMANTIC_LANCE_DB_URI` points at the published *root*. A
`latest.json` manifest under that root (written by the `semantic_search_lancedb`
job) is resolved to the current immutable `versions/<version>/` directory before
downloading, so a mid-build publish never exposes a half-written database. When
no manifest is present (legacy layout) the root directory is downloaded as-is.
"""

import json
import os
import shutil
import time

import pyarrow.fs as pafs
from loguru import logger

# Multiplicative safety margin required on top of the raw DB size before we
# attempt the download (index rebuild / temp files need some head-room).
DISK_SAFETY_FACTOR = 1.3

# Manifest object (published under the DB root) pointing at the current version.
MANIFEST_FILENAME = "latest.json"

GCS_SCHEME = "gs://"


def _gcs_key(gcs_uri: str) -> str:
    """Strip the `gs://` scheme; pyarrow's GcsFileSystem wants `bucket/key`."""
    if not gcs_uri.startswith(GCS_SCHEME):
        raise ValueError(f"Expected a {GCS_SCHEME} URI, got: {gcs_uri!r}")
    return gcs_uri[len(GCS_SCHEME) :].rstrip("/")


def _manifest_fs_and_path(uri: str) -> tuple[pafs.FileSystem, str]:
    """Resolve a ``gs://`` or local URI to a ``(filesystem, path)`` pair.

    ``gs://`` URIs use ``GcsFileSystem()`` directly so the container's default
    service-account credentials are used; everything else (local / tests) is
    delegated to ``from_uri``.
    """
    if uri.startswith(GCS_SCHEME):
        return pafs.GcsFileSystem(), _gcs_key(uri)
    fs, path = pafs.FileSystem.from_uri(uri)
    return fs, path.rstrip("/")


def _resolve_published_uri(gcs_uri: str) -> str:
    """Resolve `<root>/latest.json` to the current immutable version dir.

    Reads the manifest published by the `semantic_search_lancedb` job and
    returns the `uri` it points at. Falls back to `gcs_uri` itself when no
    manifest is found (legacy layout where the DB files lived directly under the
    root), keeping already-deployed endpoints working during the migration.
    """
    fs, root_path = _manifest_fs_and_path(gcs_uri)
    manifest_path = f"{root_path}/{MANIFEST_FILENAME}"
    if fs.get_file_info(manifest_path).type != pafs.FileType.File:
        logger.info(
            f"No {MANIFEST_FILENAME} under {gcs_uri}; downloading the directory as-is."
        )
        return gcs_uri
    with fs.open_input_stream(manifest_path) as stream:
        manifest = json.loads(stream.read().decode("utf-8"))
    version_uri = manifest["uri"]
    logger.info(
        f"Resolved semantic DB manifest: version={manifest.get('version')} "
        f"uri={version_uri}"
    )
    return version_uri


def _remote_files(fs: pafs.FileSystem, root: str) -> list[pafs.FileInfo]:
    """Every file (not directory) under `root` on the given filesystem."""
    infos = fs.get_file_info(pafs.FileSelector(root, recursive=True))
    return [info for info in infos if info.type == pafs.FileType.File]


def _validate_remote_files(files: list[pafs.FileInfo], gcs_uri: str) -> None:
    """Fail fast when the configured source does not contain any files."""
    if not files:
        raise RuntimeError(
            f"No files found under {gcs_uri}. Check the SEMANTIC_LANCE_DB_URI "
            f"config; the semantic LanceDB may not have been published yet."
        )


def _check_disk_space(
    files: list[pafs.FileInfo], gcs_uri: str, local_path: str
) -> None:
    """Ensure the destination has room for the database and safety margin."""
    needed = sum(info.size for info in files)
    parent = os.path.dirname(os.path.abspath(local_path)) or "."
    os.makedirs(parent, exist_ok=True)
    free = shutil.disk_usage(parent).free
    logger.info(
        f"Semantic DB download: source={gcs_uri} size={needed / 1e9:.2f} GB, "
        f"free disk at {parent}={free / 1e9:.2f} GB "
        f"(need {needed * DISK_SAFETY_FACTOR / 1e9:.2f} GB incl. x{DISK_SAFETY_FACTOR} margin)"
    )
    if free < needed * DISK_SAFETY_FACTOR:
        raise RuntimeError(
            f"Not enough local disk to download the semantic LanceDB: need "
            f"~{needed * DISK_SAFETY_FACTOR / 1e9:.2f} GB, only {free / 1e9:.2f} GB free "
            f"at {parent}. Use a larger machine type or read the DB directly from GCS."
        )


def _prepare_local_directory(
    files: list[pafs.FileInfo], root: str, local_path: str
) -> None:
    """Create a clean local directory with all nested destination directories."""
    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    os.makedirs(local_path, exist_ok=True)
    for info in files:
        rel = os.path.relpath(info.path, root)
        dest_dir = os.path.dirname(os.path.join(local_path, rel))
        os.makedirs(dest_dir, exist_ok=True)


def _download_files(root: str, local_path: str, gcs: pafs.FileSystem) -> None:
    """Copy the remote database files into the prepared local directory."""
    start = time.time()
    logger.info(f"Downloading semantic LanceDB to {local_path}...")
    pafs.copy_files(
        source=root,
        destination=local_path,
        source_filesystem=gcs,
        destination_filesystem=pafs.LocalFileSystem(),
    )
    logger.info(f"Semantic LanceDB downloaded in {time.time() - start:.1f}s.")


def ensure_local_semantic_db(gcs_uri: str, local_path: str) -> str:
    """Download the semantic LanceDB directory from GCS to `local_path` once.

    Args:
        gcs_uri: `gs://…` directory holding the published semantic LanceDB. May
            be the artifact *root* (containing `latest.json` + `versions/`, the
            manifest is resolved to the current version) or, for the legacy
            layout, the directory that directly contains `items.lance/`.
        local_path: Local directory to populate (opened afterwards with
            `lancedb.connect(local_path)`).

    Returns:
        `local_path` (for convenience).

    Raises:
        RuntimeError: If the local disk lacks room for the download + margin.
    """
    resolved_uri = _resolve_published_uri(gcs_uri)
    root = _gcs_key(resolved_uri)
    gcs = pafs.GcsFileSystem()

    files = _remote_files(gcs, root)
    _validate_remote_files(files, resolved_uri)
    _check_disk_space(files, resolved_uri, local_path)
    _prepare_local_directory(files, root, local_path)
    _download_files(root, local_path, gcs)
    return local_path
