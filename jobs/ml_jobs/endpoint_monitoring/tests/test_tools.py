from unittest.mock import patch

from utils.tools import download_model


def test_download_model_passes_uri_as_single_argument():
    uri = "gs://bucket/model; rm -rf /"
    with patch("utils.tools.subprocess.Popen") as mock_popen:
        mock_popen.return_value.stdout = [b"done\n"]
        download_model(artifact_uri=uri)

    args, kwargs = mock_popen.call_args
    assert args[0] == ["gsutil", "-m", "cp", "-r", uri, "."]
    assert not kwargs.get("shell")
