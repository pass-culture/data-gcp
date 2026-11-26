from unittest.mock import patch

from cli.build_and_push_docker_image import deploy_container
from constants import ENV_SHORT_NAME, GCP_PROJECT


def test_deploy_container_passes_arguments_as_list():
    with patch("cli.build_and_push_docker_image.subprocess.run") as mock_run:
        deploy_container("europe-docker.pkg.dev/img:tag", workers=1)

    args, kwargs = mock_run.call_args
    assert args[0] == [
        "sh",
        "cli/deploy_to_docker_registery.sh",
        "europe-docker.pkg.dev/img:tag",
        "1",
        GCP_PROJECT,
        ENV_SHORT_NAME,
    ]
    assert not kwargs.get("shell")
