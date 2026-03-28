from unittest.mock import patch

import pytest
import yaml

from domain.permissions import (
    _apply_permissions_to_graph,
    _process_collection,
    build_expected_graph,
    get_all_subcollections,
    get_config_path,
    load_permissions_config,
    sync_permissions,
)


class TestGetConfigPath:
    def test_dev_returns_staging(self):
        path = get_config_path()
        assert path.name == "staging.yaml"

    @patch("domain.permissions.ENVIRONMENT_SHORT_NAME", "prod")
    def test_prod_returns_production(self):
        path = get_config_path()
        assert path.name == "production.yaml"

    @patch("domain.permissions.ENVIRONMENT_SHORT_NAME", "unknown")
    def test_unknown_env_raises(self):
        with pytest.raises(ValueError, match="No permissions config"):
            get_config_path()


class TestLoadPermissionsConfig:
    def test_load_with_explicit_path(self, tmp_path):
        config = {"collections": {"root": {"id": 1}}}
        yaml_path = tmp_path / "test.yaml"
        yaml_path.write_text(yaml.dump(config))

        result = load_permissions_config(yaml_path)
        assert result == config

    def test_load_default_path(self):
        result = load_permissions_config()
        assert "collections" in result


class TestGetAllSubcollections:
    def test_no_children(self, metabase):
        metabase.get_collection_children.return_value = {"data": []}
        result = get_all_subcollections(metabase, 100)
        assert result == []

    def test_nested_children(self, metabase):
        def side_effect(collection_id, models=None):
            if collection_id == 100:
                return {"data": [{"model": "collection", "id": 200}]}
            elif collection_id == 200:
                return {"data": [{"model": "collection", "id": 300}]}
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect
        result = get_all_subcollections(metabase, 100)
        assert result == [200, 300]

    def test_non_collection_items_skipped(self, metabase):
        metabase.get_collection_children.return_value = {
            "data": [{"model": "card", "id": 999}]
        }
        result = get_all_subcollections(metabase, 100)
        assert result == []


class TestApplyPermissionsToGraph:
    def test_applies_change(self):
        graph = {"5": {"10": "read"}}
        changes = _apply_permissions_to_graph(graph, 10, {5: "write"})
        assert len(changes) == 1
        assert changes[0]["old"] == "read"
        assert changes[0]["new"] == "write"
        assert graph["5"]["10"] == "write"

    def test_no_change_when_same(self):
        graph = {"5": {"10": "write"}}
        changes = _apply_permissions_to_graph(graph, 10, {5: "write"})
        assert changes == []

    def test_missing_group_raises(self):
        graph = {"5": {"10": "read"}}
        with pytest.raises(ValueError, match="Group 99 not found"):
            _apply_permissions_to_graph(graph, 10, {99: "write"})

    def test_new_collection_in_group(self):
        graph = {"5": {}}
        changes = _apply_permissions_to_graph(graph, 10, {5: "write"})
        assert len(changes) == 1
        assert changes[0]["old"] is None
        assert changes[0]["new"] == "write"


class TestProcessCollection:
    def test_simple_collection(self, metabase):
        graph = {"5": {"10": "read"}}
        config = {"id": 10, "permissions": {5: "write"}}
        changes = _process_collection("root", config, graph, metabase)
        assert len(changes) == 1

    def test_enforce_on_children(self, metabase):
        graph = {"5": {"10": "read", "20": "read"}}
        config = {
            "id": 10,
            "permissions": {5: "write"},
            "enforce_on_children": True,
        }
        metabase.get_collection_children.return_value = {
            "data": [{"model": "collection", "id": 20}]
        }

        # First call for subcollections (returns 20), second for 20's children (empty)
        def side_effect(collection_id, models=None):
            if collection_id == 10:
                return {"data": [{"model": "collection", "id": 20}]}
            return {"data": []}

        metabase.get_collection_children.side_effect = side_effect

        changes = _process_collection("root", config, graph, metabase)
        # Both 10 and 20 should be changed
        assert len(changes) == 2

    def test_explicit_children(self, metabase):
        graph = {"5": {"10": "read", "20": "read"}}
        config = {
            "id": 10,
            "permissions": {5: "write"},
            "children": {
                "child": {"id": 20, "permissions": {5: "none"}},
            },
        }

        changes = _process_collection("root", config, graph, metabase)
        assert len(changes) == 2
        # Child should have "none"
        child_change = [c for c in changes if c["collection_id"] == 20][0]
        assert child_change["new"] == "none"

    def test_no_permissions_key(self, metabase):
        graph = {"5": {"10": "read"}}
        config = {"id": 10}
        changes = _process_collection("root", config, graph, metabase)
        assert changes == []


class TestBuildExpectedGraph:
    def test_processes_all_collections(self, metabase):
        graph = {"5": {"10": "read", "20": "read"}}
        config = {
            "collections": {
                "col_a": {"id": 10, "permissions": {5: "write"}},
                "col_b": {"id": 20, "permissions": {5: "none"}},
            }
        }

        changes = build_expected_graph(config, graph, metabase)
        assert len(changes) == 2


class TestSyncPermissions:
    def test_no_changes(self, metabase, tmp_path):
        config = {"collections": {"root": {"id": 10, "permissions": {5: "read"}}}}
        yaml_path = tmp_path / "perms.yaml"
        yaml_path.write_text(yaml.dump(config))

        metabase.get_collection_graph.return_value = {
            "revision": 1,
            "groups": {"5": {"10": "read"}},
        }

        result = sync_permissions(metabase, yaml_path=yaml_path)
        assert result == {"changes": [], "applied": False}
        metabase.put_collection_graph.assert_not_called()

    def test_applies_changes(self, metabase, tmp_path):
        config = {"collections": {"root": {"id": 10, "permissions": {5: "write"}}}}
        yaml_path = tmp_path / "perms.yaml"
        yaml_path.write_text(yaml.dump(config))

        metabase.get_collection_graph.return_value = {
            "revision": 1,
            "groups": {"5": {"10": "read"}},
        }

        result = sync_permissions(metabase, yaml_path=yaml_path)
        assert result["applied"] is True
        assert len(result["changes"]) == 1
        metabase.put_collection_graph.assert_called_once()

    def test_dry_run(self, metabase, tmp_path):
        config = {"collections": {"root": {"id": 10, "permissions": {5: "write"}}}}
        yaml_path = tmp_path / "perms.yaml"
        yaml_path.write_text(yaml.dump(config))

        metabase.get_collection_graph.return_value = {
            "revision": 1,
            "groups": {"5": {"10": "read"}},
        }

        result = sync_permissions(metabase, yaml_path=yaml_path, dry_run=True)
        assert result["applied"] is False
        assert len(result["changes"]) == 1
        metabase.put_collection_graph.assert_not_called()

    def test_default_yaml_path(self, metabase):
        # Staging YAML uses groups 2 and 1 on root collection
        metabase.get_collection_graph.return_value = {
            "revision": 1,
            "groups": {
                "2": {"root": "write"},
                "1": {"root": "none"},
            },
        }

        result = sync_permissions(metabase)
        assert result == {"changes": [], "applied": False}

    def test_missing_group_raises(self, metabase, tmp_path):
        config = {"collections": {"root": {"id": 10, "permissions": {99: "write"}}}}
        yaml_path = tmp_path / "perms.yaml"
        yaml_path.write_text(yaml.dump(config))

        metabase.get_collection_graph.return_value = {
            "revision": 1,
            "groups": {"5": {"10": "read"}},
        }

        with pytest.raises(ValueError, match="Group 99 not found"):
            sync_permissions(metabase, yaml_path=yaml_path)
