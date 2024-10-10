from unittest.mock import patch

from docarray import DocumentArray

from app.retrieval.utils import load_documents


@patch("docarray.DocumentArray.load")
def test_load_documents(mock_load):
    mock_docarray = DocumentArray()
    mock_load.return_value = mock_docarray

    path = "fake_path"
    result = load_documents(path)

    mock_load.assert_called_once_with(path)

    assert result == mock_docarray
