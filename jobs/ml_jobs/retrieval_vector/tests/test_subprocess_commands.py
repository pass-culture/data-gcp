from unittest.mock import patch

from src.docker import deploy_container
from src.gcs_io import download_model


def test_download_model_passes_uri_as_single_argument():
    uri = "gs://bucket/model; rm -rf /"
    with patch("src.gcs_io.subprocess.run") as mock_run:
        download_model(artifact_uri=uri)

    args, kwargs = mock_run.call_args
    assert args[0] == ["gsutil", "-m", "cp", "-r", uri, "."]
    assert not kwargs.get("shell")


def test_deploy_container_passes_arguments_as_list():
    with patch("src.docker.subprocess.run") as mock_run:
        deploy_container("europe-docker.pkg.dev/img:tag", workers=2)

    args, kwargs = mock_run.call_args
    assert args[0] == [
        "sh",
        "./deploy_to_docker_registery.sh",
        "europe-docker.pkg.dev/img:tag",
        "2",
    ]
    assert not kwargs.get("shell")
