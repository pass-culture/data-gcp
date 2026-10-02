"""Unit tests for the semantic LanceDB manifest resolution.

These exercise ``_resolve_published_uri`` against a local filesystem (via
``file://`` URIs) so no GCS access is needed. The actual download path
(``ensure_local_semantic_db``) is GCS-only and covered at integration time.
"""

import json

from app.retrieval.db_bootstrap import _resolve_published_uri


def test_resolve_published_uri_reads_manifest(tmp_path):
    version_dir = tmp_path / "versions" / "20260102T120000"
    version_dir.mkdir(parents=True)
    manifest = {
        "version": "20260102T120000",
        "uri": str(version_dir),
        "table": "items",
        "row_count": 10,
    }
    (tmp_path / "latest.json").write_text(json.dumps(manifest))

    resolved = _resolve_published_uri(f"file://{tmp_path}")

    assert resolved == str(version_dir)


def test_resolve_published_uri_falls_back_without_manifest(tmp_path):
    uri = f"file://{tmp_path}"

    assert _resolve_published_uri(uri) == uri
