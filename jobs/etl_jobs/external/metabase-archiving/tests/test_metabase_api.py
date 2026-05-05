from unittest.mock import MagicMock, patch

import pytest


def _make_api(mock_requests, mock_id_token):
    """Instantiate MetabaseAPI with mocked auth."""
    mock_id_token.fetch_id_token.return_value = "fake-token"

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "session-123"}
    mock_requests.post.return_value = mock_response

    from core.metabase_api import MetabaseAPI

    return MetabaseAPI(
        username="user@test.com",
        password="pass",
        host="https://metabase.test",
        client_id="client-id",
    )


@pytest.fixture
def api():
    with (
        patch("core.metabase_api.requests") as mock_requests,
        patch("core.metabase_api.id_token") as mock_id_token,
    ):
        yield _make_api(mock_requests, mock_id_token), mock_requests


class TestMetabaseAPIInit:
    def test_successful_login(self):
        with (
            patch("core.metabase_api.requests") as mock_requests,
            patch("core.metabase_api.id_token") as mock_id_token,
        ):
            api = _make_api(mock_requests, mock_id_token)

            assert api.host == "https://metabase.test"
            assert api.bearer_token == "Bearer fake-token"
            assert api.headers["X-Metabase-Session"] == "session-123"

    def test_login_no_id_in_response(self):
        with (
            patch("core.metabase_api.requests") as mock_requests,
            patch("core.metabase_api.id_token") as mock_id_token,
        ):
            mock_id_token.fetch_id_token.return_value = "fake-token"
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"error": "bad"}
            mock_requests.post.return_value = mock_response

            from core.metabase_api import MetabaseAPI

            with pytest.raises(Exception, match="Error login"):
                MetabaseAPI(username="u", password="p", host="https://h", client_id="c")

    def test_login_204_status(self):
        with (
            patch("core.metabase_api.requests") as mock_requests,
            patch("core.metabase_api.id_token") as mock_id_token,
        ):
            mock_id_token.fetch_id_token.return_value = "fake-token"
            mock_response = MagicMock()
            mock_response.status_code = 204
            mock_requests.post.return_value = mock_response

            from core.metabase_api import MetabaseAPI

            api = MetabaseAPI(
                username="u", password="p", host="https://h", client_id="c"
            )
            assert api.host == "https://h"
            # headers not set on 204
            assert not hasattr(api, "headers")


class TestMetabaseAPIMethods:
    def test_get_users(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = {"data": [{"id": 1}]}

        result = instance.get_users()
        assert result == [{"id": 1}]

    def test_put_card(self, api):
        instance, mock_requests = api
        mock_requests.put.return_value.json.return_value = {"ok": True}

        result = instance.put_card(42, {"name": "test"})
        assert result == {"ok": True}
        mock_requests.put.assert_called_once()

    def test_update_card_collections(self, api):
        instance, mock_requests = api
        mock_requests.post.return_value.json.return_value = {"status": "ok"}

        result = instance.update_card_collections([1, 2], 99)
        assert result == {"status": "ok"}

    def test_get_cards_with_id(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = {"id": 5}

        result = instance.get_cards(5)
        assert result == {"id": 5}
        assert "/api/card/5" in mock_requests.get.call_args[0][0]

    def test_get_cards_without_id(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = [{"id": 1}]

        result = instance.get_cards()
        assert result == [{"id": 1}]
        assert "/api/card/" in mock_requests.get.call_args[0][0]

    def test_get_table_with_id(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = {"id": 10}

        result = instance.get_table(10)
        assert result == {"id": 10}
        assert "/api/table/10" in mock_requests.get.call_args[0][0]

    def test_get_table_without_id(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = [{"id": 1}]

        result = instance.get_table()
        assert result == [{"id": 1}]
        assert "/api/table/" in mock_requests.get.call_args[0][0]

    def test_get_collections_with_id(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = {"id": 3}

        result = instance.get_collections(3)
        assert result == {"id": 3}

    def test_get_collections_without_id(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = [{"id": 1}]

        result = instance.get_collections()
        assert result == [{"id": 1}]

    def test_get_dashboards_with_id(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = {"id": 7}

        result = instance.get_dashboards(7)
        assert result == {"id": 7}

    def test_get_dashboards_without_id(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = [{"id": 1}]

        result = instance.get_dashboards()
        assert result == [{"id": 1}]

    def test_put_collection(self, api):
        instance, mock_requests = api
        mock_requests.put.return_value.json.return_value = {"ok": True}

        result = instance.put_collection(5, {"archived": True})
        assert result == {"ok": True}

    def test_put_dashboard(self, api):
        instance, mock_requests = api
        mock_requests.put.return_value.json.return_value = {"ok": True}

        result = instance.put_dashboard(5, {"archived": True})
        assert result == {"ok": True}

    def test_get_bookmarks(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = [{"id": 1}]

        result = instance.get_bookmarks()
        assert result == [{"id": 1}]

    def test_get_collection_graph(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = {"groups": {}}

        result = instance.get_collection_graph()
        assert result == {"groups": {}}

    def test_put_collection_graph(self, api):
        instance, mock_requests = api
        mock_requests.put.return_value.json.return_value = {"ok": True}

        result = instance.put_collection_graph({"groups": {}})
        assert result == {"ok": True}

    def test_get_collection_children_without_models(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = {"data": []}

        result = instance.get_collection_children(10)
        assert result == {"data": []}

    def test_get_collection_children_with_models(self, api):
        instance, mock_requests = api
        mock_requests.get.return_value.json.return_value = {"data": []}

        result = instance.get_collection_children(10, models=["collection"])
        assert result == {"data": []}
        # Verify params passed
        _, kwargs = mock_requests.get.call_args
        assert kwargs["params"] == {"models": ["collection"]}


class TestFormatCards:
    def test_format_native_card_with_last_edit(self, api):
        instance, _ = api
        cards = [
            {
                "id": 1,
                "name": "Card 1",
                "description": "desc",
                "archived": False,
                "creator": {"id": 10},
                "updated_at": "2024-01-01",
                "database_id": 2,
                "query_type": "native",
                "query": "SELECT 1",
                "legacy_query": {"native": {"query": "SELECT 1"}},
                "last-edit-info": {
                    "id": 10,
                    "email": "user@test.com",
                    "timestamp": "2024-01-01",
                },
            }
        ]
        result = instance.format_cards(cards)
        assert len(result) == 1
        assert result[0]["card_id"] == 1
        assert result[0]["card_dataset_query"] == "SELECT 1"
        assert result[0]["card_last_edit_email"] == "user@test.com"

    def test_format_query_card_without_last_edit(self, api):
        instance, _ = api
        cards = [
            {
                "id": 2,
                "name": "Card 2",
                "description": "desc",
                "archived": False,
                "creator_id": 5,
                "updated_at": "2024-01-01",
                "database_id": 2,
                "query_type": "query",
                "query": "something",
                "legacy_query": {"query": {"source-table": 1}},
            }
        ]
        result = instance.format_cards(cards)
        assert len(result) == 1
        assert result[0]["card_creator_id"] == 5
        assert result[0]["card_dataset_query"] is None

    def test_format_card_without_query_key(self, api):
        instance, _ = api
        cards = [{"id": 3, "name": "No query"}]
        result = instance.format_cards(cards)
        assert result == []


class TestExportDashboards:
    def test_export_dashboards(self, api):
        instance, mock_requests = api

        dashboards = [{"id": 1, "creator": {"id": 10}, "query_average_duration": 100}]

        dash_details = {
            "id": 1,
            "name": "Dash 1",
            "archived": False,
            "collection_position": None,
            "created_at": "2024-01-01",
            "cache_ttl": None,
            "last-edit-info": {
                "timestamp": "2024-01-02",
                "email": "u@t.com",
                "id": 10,
            },
            "ordered_cards": [
                {
                    "card": {
                        "id": 100,
                        "name": "C1",
                        "description": "d",
                        "archived": False,
                        "creator": {"id": 10},
                        "updated_at": "2024-01-01",
                        "database_id": 2,
                        "query_type": "native",
                        "query": "SELECT 1",
                        "legacy_query": {"native": {"query": "SELECT 1"}},
                    }
                }
            ],
        }
        mock_requests.get.return_value.json.return_value = dash_details

        with patch("core.metabase_api.sleep"):
            result = instance.export_dashboards(dashboards, timeout_sleep=0)

        assert len(result) == 1
        assert result[0]["dashboard_id"] == 1
        assert result[0]["card_id"] == 100
        assert result[0]["dashboard_last_edit_email"] == "u@t.com"

    def test_export_dashboards_without_last_edit(self, api):
        instance, mock_requests = api

        dashboards = [{"id": 2, "creator_id": 5}]
        dash_details = {
            "id": 2,
            "name": "Dash 2",
            "archived": False,
            "collection_position": None,
            "created_at": "2024-01-01",
            "cache_ttl": None,
            "ordered_cards": [],
        }
        mock_requests.get.return_value.json.return_value = dash_details

        with patch("core.metabase_api.sleep"):
            result = instance.export_dashboards(dashboards, timeout_sleep=0)

        assert result == []
