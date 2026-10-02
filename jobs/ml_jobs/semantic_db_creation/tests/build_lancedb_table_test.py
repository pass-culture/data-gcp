import json

from build_lancedb_table import (
    MANIFEST_FILENAME,
    prune_old_versions,
    versioned_uri,
    write_latest_manifest,
)


def test_versioned_uri_builds_immutable_path():
    assert versioned_uri("gs://b/semantic_search_lancedb/", "20260102T120000") == (
        "gs://b/semantic_search_lancedb/versions/20260102T120000"
    )


def test_write_latest_manifest(tmp_path):
    root = f"file://{tmp_path}"
    version_uri = versioned_uri(root, "20260102T120000")

    write_latest_manifest(root, "20260102T120000", version_uri, "items", 42)

    manifest = json.loads((tmp_path / MANIFEST_FILENAME).read_text())
    assert manifest["version"] == "20260102T120000"
    assert manifest["uri"] == version_uri
    assert manifest["table"] == "items"
    assert manifest["row_count"] == 42
    assert "created_at" in manifest


def test_prune_old_versions_keeps_most_recent(tmp_path):
    versions = tmp_path / "versions"
    names = [
        "20260101T000000",
        "20260102T000000",
        "20260103T000000",
        "20260104T000000",
    ]
    for name in names:
        (versions / name).mkdir(parents=True)

    prune_old_versions(f"file://{tmp_path}", keep=2)

    remaining = sorted(p.name for p in versions.iterdir())
    assert remaining == ["20260103T000000", "20260104T000000"]


def test_prune_old_versions_disabled_when_keep_is_zero(tmp_path):
    versions = tmp_path / "versions"
    (versions / "20260101T000000").mkdir(parents=True)

    prune_old_versions(f"file://{tmp_path}", keep=0)

    assert (versions / "20260101T000000").exists()
