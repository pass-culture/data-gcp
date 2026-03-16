from pathlib import Path
from unittest.mock import MagicMock, patch


class TestModuleConstants:
    def test_project_name(self):
        from core.utils import PROJECT_NAME

        assert PROJECT_NAME == "test-project"

    def test_environment_short_name(self):
        from core.utils import ENVIRONMENT_SHORT_NAME

        assert ENVIRONMENT_SHORT_NAME == "dev"

    def test_environment_long_name(self):
        from core.utils import ENVIRONMENT_LONG_NAME

        assert ENVIRONMENT_LONG_NAME == "development"

    def test_int_metabase_dataset(self):
        from core.utils import INT_METABASE_DATASET

        assert INT_METABASE_DATASET == "int_metabase_dev"

    def test_metabase_api_username(self):
        from core.utils import METABASE_API_USERNAME

        assert METABASE_API_USERNAME == "metabase-data-bot@passculture.app"

    def test_config_dir_points_to_config(self):
        from core.utils import CONFIG_DIR

        assert CONFIG_DIR.name == "config"
        assert CONFIG_DIR == Path(__file__).parent.parent / "config"


class TestLoadArchivingConfig:
    def test_load_archiving_config(self):
        from core.utils import load_archiving_config

        config = load_archiving_config()
        assert "max_cards_to_archive" in config
        assert "folders" in config
        assert "rules" in config
        assert isinstance(config["folders"], list)
        assert isinstance(config["rules"], list)


class TestAccessSecretData:
    def test_returns_default_on_credentials_error(self):
        from google.auth.exceptions import DefaultCredentialsError

        from core.utils import access_secret_data

        with patch(
            "core.utils.secretmanager.SecretManagerServiceClient",
            side_effect=DefaultCredentialsError("no creds"),
        ):
            result = access_secret_data("proj", "secret", default="fallback")

        assert result == "fallback"

    def test_returns_secret_value(self):
        from core.utils import access_secret_data

        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = "my-secret"

        mock_client = MagicMock()
        mock_client.access_secret_version.return_value = mock_response

        with patch(
            "core.utils.secretmanager.SecretManagerServiceClient",
            return_value=mock_client,
        ):
            result = access_secret_data("proj", "secret_id")

        assert result == "my-secret"
        mock_client.access_secret_version.assert_called_once_with(
            request={"name": "projects/proj/secrets/secret_id/versions/latest"}
        )

    def test_returns_default_none_when_not_specified(self):
        from google.auth.exceptions import DefaultCredentialsError

        from core.utils import access_secret_data

        with patch(
            "core.utils.secretmanager.SecretManagerServiceClient",
            side_effect=DefaultCredentialsError("no creds"),
        ):
            result = access_secret_data("proj", "secret")

        assert result is None
