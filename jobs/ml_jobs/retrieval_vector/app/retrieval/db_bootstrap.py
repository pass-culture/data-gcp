"""Startup helper that materialises the semantic LanceDB on local disk.

At container startup the `SemanticClient` downloads that GCS directory to local disk once,
then opens it locally. Vector search is run against fast local storage instead of over the network.
"""

import os
import shutil
import time

import pyarrow.fs as pafs
from loguru import logger

# Multiplicative safety margin required on top of the raw DB size before we
# attempt the download (index rebuild / temp files need some head-room).
DISK_SAFETY_FACTOR = 1.3


def _gcs_key(gcs_uri: str) -> str:
    """Strip the `gs://` scheme; pyarrow's GcsFileSystem wants `bucket/key`."""
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Expected a gs:// URI, got: {gcs_uri!r}")
    return gcs_uri[len("gs://") :].rstrip("/")


def _remote_files(fs: pafs.FileSystem, root: str) -> list[pafs.FileInfo]:
    """Every file (not directory) under `root` on the given filesystem."""
    infos = fs.get_file_info(pafs.FileSelector(root, recursive=True))
    return [info for info in infos if info.type == pafs.FileType.File]


def ensure_local_semantic_db(gcs_uri: str, local_path: str) -> str:
    """Download the semantic LanceDB directory from GCS to `local_path` once.

    Args:
        gcs_uri: `gs://…` directory holding the LanceDB database (the dir that
            contains `items.lance/`), as published by `semantic_search_lancedb`.
        local_path: Local directory to populate (opened afterwards with
            `lancedb.connect(local_path)`).

    Returns:
        `local_path` (for convenience).

    Raises:
        RuntimeError: If the local disk lacks room for the download + margin.
    """
    root = _gcs_key(gcs_uri)
    gcs = pafs.GcsFileSystem()

    files = _remote_files(gcs, root)
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

    # Start from a clean directory so a partial previous download can't linger.
    if os.path.exists(local_path):
        shutil.rmtree(local_path)
    os.makedirs(local_path, exist_ok=True)

    # GCS has no real directories, so the recursive listing returns only files
    # and copy_files won't create the nested local subdirs for us. Pre-create
    # every destination directory to avoid a FileNotFoundError when copy_files
    # writes into deep paths (e.g. items.lance/_indices/<uuid>/auxiliary.idx).
    for info in files:
        rel = os.path.relpath(info.path, root)
        dest_dir = os.path.dirname(os.path.join(local_path, rel))
        os.makedirs(dest_dir, exist_ok=True)

    start = time.time()
    logger.info(f"Downloading semantic LanceDB to {local_path}...")
    pafs.copy_files(
        source=root,
        destination=local_path,
        source_filesystem=gcs,
        destination_filesystem=pafs.LocalFileSystem(),
    )
    logger.info(
        f"Semantic LanceDB downloaded in {time.time() - start:.1f}s "
        f"({needed / 1e9:.2f} GB)."
    )
    return local_path
