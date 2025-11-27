from unittest.mock import patch

import pandas as pd

from constants import WIKIPEDIA_URL_KEY
from get_wikipedia_page_content import (
    extract_wikipedia_content_from_url,
    fetch_clean_content,
)


def test_extract_wikipedia_content_from_url_decodes_titles():
    df = pd.DataFrame(
        {
            WIKIPEDIA_URL_KEY: [
                "https://sr.wikipedia.org/wiki/Mirjana_Novakovi%C4%87",
                "https://en.wikipedia.org/wiki/Simple_Title",
            ]
        }
    )

    result = extract_wikipedia_content_from_url(df)

    # Check Mirjana_Novaković
    mirjana = result[result["page_title"] == "Mirjana_Novaković"]
    assert not mirjana.empty
    assert mirjana.iloc[0]["language"] == "sr"

    # Check Simple_Title
    simple = result[result["page_title"] == "Simple_Title"]
    assert not simple.empty
    assert simple.iloc[0]["language"] == "en"


def test_fetch_clean_content():
    mock_response_data = {
        "query": {
            "pages": {
                "1": {
                    "title": "Test_Page",
                    "revisions": [
                        {
                            "slots": {
                                "main": {
                                    "*": "'''Bold Text'''\n== Section 1 ==\nContent 1.\n== References ==\nRef 1."
                                }
                            }
                        }
                    ],
                }
            }
        }
    }

    with patch("requests.post") as mock_post:
        mock_post.return_value.json.return_value = mock_response_data

        results = fetch_clean_content(["Test_Page"], "en")

        assert "Test_Page" in results
        # mwparserfromhell strip_code removes bold formatting
        assert "Bold Text" in results["Test_Page"]
        assert "Content 1." in results["Test_Page"]
        # The References section should be removed
        assert "Ref 1." not in results["Test_Page"]


def test_fetch_clean_content_with_redirect():
    mock_response_data = {
        "query": {
            "redirects": [{"from": "Old_Title", "to": "New_Title"}],
            "pages": {
                "1": {
                    "title": "New_Title",
                    "revisions": [{"slots": {"main": {"*": "Content."}}}],
                }
            },
        }
    }

    with patch("requests.post") as mock_post:
        mock_post.return_value.json.return_value = mock_response_data

        results = fetch_clean_content(["Old_Title"], "en")

        # The function should map the result back to the requested title (Old_Title)
        assert "Old_Title" in results
        assert results["Old_Title"].strip() == "Content."
