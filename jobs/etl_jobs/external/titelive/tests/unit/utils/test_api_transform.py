"""Tests for API transformation utilities."""

import json

import pandas as pd
import pytest

from src.utils.api_transform import (
    extract_gencods_from_search_response,
    transform_api_response,
)


class TestTransformApiResponse:
    """Test cases for transform_api_response function."""

    def test_transform_api_response_basic(self, sample_api_response):
        """Test basic API response transformation with dict format."""
        # Act
        result_df = transform_api_response(sample_api_response)

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert list(result_df.columns) == ["ean", "json_raw"]
        assert len(result_df) == 2  # Two unique EANs from the sample data

        # Check EANs are present
        eans = result_df["ean"].tolist()
        assert "9781234567890" in eans
        assert "9789876543210" in eans

        # Verify article is stored as list in json_raw
        for _, row in result_df.iterrows():
            parsed = json.loads(row["json_raw"])
            assert isinstance(parsed["article"], list)

    def test_transform_api_response_list_format(self, sample_api_response_list_format):
        """Test transformation with article as list instead of dict."""
        # Act
        result_df = transform_api_response(sample_api_response_list_format)

        # Assert
        assert len(result_df) == 1
        assert result_df["ean"].iloc[0] == "9781111111111"

        # Verify article is stored as list in json_raw
        parsed = json.loads(result_df["json_raw"].iloc[0])
        assert isinstance(parsed["article"], list)

    def test_transform_api_response_dict_result(self, sample_api_response_dict_result):
        """Test transformation when result is a dict instead of list."""
        # Act
        result_df = transform_api_response(sample_api_response_dict_result)

        # Assert
        assert len(result_df) == 1
        assert result_df["ean"].iloc[0] == "9782222222222"

        # Verify article is stored as list in json_raw
        parsed = json.loads(result_df["json_raw"].iloc[0])
        assert isinstance(parsed["article"], list)

    def test_transform_api_response_oeuvre_key(self):
        """Test transformation when response has 'oeuvre' key instead of 'result'."""
        # Arrange
        api_response = {
            "oeuvre": [
                {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": "9783333333333",
                            "datemodification": "20/10/2024",
                            "titre": "Music Album",
                        }
                    },
                }
            ]
        }

        # Act
        result_df = transform_api_response(api_response)

        # Assert
        assert len(result_df) == 1
        assert result_df["ean"].iloc[0] == "9783333333333"

        # Verify article is stored as list in json_raw
        parsed = json.loads(result_df["json_raw"].iloc[0])
        assert isinstance(parsed["article"], list)

    def test_transform_api_response_empty_result(self):
        """Test transformation with empty result."""
        # Arrange
        api_response = {"result": []}

        # Act
        result_df = transform_api_response(api_response)

        # Assert
        assert isinstance(result_df, pd.DataFrame)
        assert len(result_df) == 0
        assert list(result_df.columns) == ["ean", "json_raw"]

    def test_transform_api_response_missing_result_key(self):
        """Test error handling when 'result' key is missing."""
        # Arrange
        api_response = {"data": []}

        # Act & Assert
        with pytest.raises(
            ValueError, match="Invalid API response format: missing 'result' key"
        ):
            transform_api_response(api_response)

    def test_transform_api_response_invalid_date_format(self):
        """Test that articles are processed regardless of date format."""
        # Arrange
        api_response = {
            "result": [
                {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": "9784444444444",
                            # Any date format is fine now since we don't parse it
                            "datemodification": "2024-10-20",
                            "titre": "Test",
                        }
                    },
                }
            ]
        }

        # Act
        result_df = transform_api_response(api_response)

        # Assert - Should process article since we only need EAN
        assert len(result_df) == 1
        assert result_df["ean"].iloc[0] == "9784444444444"

    def test_transform_api_response_missing_required_fields(self):
        """Test handling when article is missing EAN."""
        # Arrange
        api_response = {
            "result": [
                {
                    "id": 1,
                    "article": {
                        "1": {"datemodification": "20/10/2024", "titre": "Missing EAN"}
                    },
                },
                {
                    "id": 2,
                    "article": {"1": {"gencod": "9785555555555", "titre": "Has EAN"}},
                },
            ]
        }

        # Act
        result_df = transform_api_response(api_response)

        # Assert - Should skip article without EAN, but process article with EAN
        assert len(result_df) == 1
        assert result_df["ean"].iloc[0] == "9785555555555"

    def test_json_raw_article_format_is_list(self, sample_api_response):
        """Test that json_raw always contains articles as list, not dict."""
        # Act
        result_df = transform_api_response(sample_api_response)

        # Assert - Check that all json_raw entries have article as list
        for _, row in result_df.iterrows():
            parsed = json.loads(row["json_raw"])
            assert "article" in parsed
            assert isinstance(
                parsed["article"], list
            ), "Article should be a list, not a dict"
            # Verify list contains dict objects
            assert all(isinstance(item, dict) for item in parsed["article"])


class TestExtractGencodsFromSearchResponse:
    """Test cases for extract_gencods_from_search_response function."""

    def test_extract_gencods_basic(self):
        """Test basic gencod extraction from search response with date filter."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": "9781234567890",
                            "datemodification": "15/10/2024",
                            "titre": "Book 1",
                        },
                        "2": {
                            "gencod": "9781234567891",
                            "datemodification": "16/10/2024",
                            "titre": "Book 1 Edition 2",
                        },
                    },
                },
                {
                    "id": 456,
                    "article": {
                        "1": {
                            "gencod": "9789876543210",
                            "datemodification": "17/10/2024",
                            "titre": "Book 2",
                        }
                    },
                },
            ]
        }

        # Act
        gencods = extract_gencods_from_search_response(
            search_response, from_date="10/10/2024"
        )

        # Assert
        assert len(gencods) == 3
        assert "9781234567890" in gencods
        assert "9781234567891" in gencods
        assert "9789876543210" in gencods

    def test_extract_gencods_with_duplicates(self):
        """Test that duplicate gencods are removed."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": "9781234567890",
                            "datemodification": "15/10/2024",
                            "titre": "Book 1",
                        },
                        "2": {
                            "gencod": "9781234567890",
                            "datemodification": "16/10/2024",
                            "titre": "Book 1 Duplicate",
                        },
                    },
                },
                {
                    "id": 456,
                    "article": {
                        "1": {
                            "gencod": "9781234567890",
                            "datemodification": "17/10/2024",
                            "titre": "Book 1",
                        }
                    },
                },
            ]
        }

        # Act
        gencods = extract_gencods_from_search_response(
            search_response, from_date="10/10/2024"
        )

        # Assert
        assert len(gencods) == 1
        assert "9781234567890" in gencods

    def test_extract_gencods_list_format(self):
        """Test gencod extraction when article is a list."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": [
                        {
                            "gencod": "9781234567890",
                            "datemodification": "15/10/2024",
                            "titre": "Book 1",
                        },
                        {
                            "gencod": "9781234567891",
                            "datemodification": "16/10/2024",
                            "titre": "Book 2",
                        },
                    ],
                }
            ]
        }

        # Act
        gencods = extract_gencods_from_search_response(
            search_response, from_date="10/10/2024"
        )

        # Assert
        assert len(gencods) == 2
        assert "9781234567890" in gencods
        assert "9781234567891" in gencods

    def test_extract_gencods_dict_result(self):
        """Test gencod extraction when result is a dict instead of list."""
        # Arrange
        search_response = {
            "result": {
                "0": {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": "9781234567890",
                            "datemodification": "15/10/2024",
                            "titre": "Book 1",
                        }
                    },
                }
            }
        }

        # Act
        gencods = extract_gencods_from_search_response(
            search_response, from_date="10/10/2024"
        )

        # Assert
        assert len(gencods) == 1
        assert "9781234567890" in gencods

    def test_extract_gencods_empty_result(self):
        """Test gencod extraction with empty result."""
        # Arrange
        search_response = {"result": []}

        # Act
        gencods = extract_gencods_from_search_response(
            search_response, from_date="10/10/2024"
        )

        # Assert
        assert len(gencods) == 0

    def test_extract_gencods_missing_result_key(self):
        """Test error handling when 'result' key is missing."""
        # Arrange
        search_response = {"data": []}

        # Act & Assert
        with pytest.raises(
            ValueError, match="Invalid API response format: missing 'result' key"
        ):
            extract_gencods_from_search_response(
                search_response, from_date="10/10/2024"
            )

    def test_extract_gencods_missing_gencod_field(self):
        """Test handling when some articles are missing gencod."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": {
                        "1": {"datemodification": "15/10/2024", "titre": "No Gencod"},
                        "2": {
                            "gencod": "9781234567890",
                            "datemodification": "16/10/2024",
                            "titre": "Has Gencod",
                        },
                    },
                }
            ]
        }

        # Act
        gencods = extract_gencods_from_search_response(
            search_response, from_date="10/10/2024"
        )

        # Assert
        assert len(gencods) == 1
        assert "9781234567890" in gencods

    def test_extract_gencods_invalid_article_format(self):
        """Test handling when article is neither dict nor list."""
        # Arrange
        search_response = {"result": [{"id": 123, "article": "invalid"}]}

        # Act
        gencods = extract_gencods_from_search_response(
            search_response, from_date="10/10/2024"
        )

        # Assert
        assert len(gencods) == 0

    def test_extract_gencods_numeric_gencod(self):
        """Test that numeric gencods are converted to strings."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": 9781234567890,
                            "datemodification": "15/10/2024",
                            "titre": "Book",
                        }
                    },
                }
            ]
        }

        # Act
        gencods = extract_gencods_from_search_response(
            search_response, from_date="10/10/2024"
        )

        # Assert
        assert len(gencods) == 1
        assert "9781234567890" in gencods
        assert isinstance(gencods[0], str)

    def test_extract_gencods_with_date_filter_includes_matching(self):
        """Test that articles with datemodification >= from_date are included."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": "9781234567890",
                            "datemodification": "15/10/2024",
                        },
                        "2": {
                            "gencod": "9781234567891",
                            "datemodification": "20/10/2024",
                        },
                    },
                }
            ]
        }

        # Act - Filter from 15/10/2024
        gencods = extract_gencods_from_search_response(
            search_response, from_date="15/10/2024"
        )

        # Assert - Both should be included (>= from_date)
        assert len(gencods) == 2
        assert "9781234567890" in gencods
        assert "9781234567891" in gencods

    def test_extract_gencods_with_date_filter_excludes_old(self):
        """Test that articles with datemodification < from_date are excluded."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": "9781234567890",
                            "datemodification": "10/10/2024",
                        },
                        "2": {
                            "gencod": "9781234567891",
                            "datemodification": "20/10/2024",
                        },
                    },
                }
            ]
        }

        # Act - Filter from 15/10/2024
        gencods = extract_gencods_from_search_response(
            search_response, from_date="15/10/2024"
        )

        # Assert - Only second article should be included
        assert len(gencods) == 1
        assert "9781234567891" in gencods
        assert "9781234567890" not in gencods

    def test_extract_gencods_with_date_filter_includes_missing_date(self):
        """Test that articles without datemodification are included when filtering."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": "9781234567890",
                            "datemodification": "10/10/2024",
                        },
                        "2": {
                            "gencod": "9781234567891",
                            # No datemodification field
                        },
                    },
                }
            ]
        }

        # Act - Filter from 15/10/2024
        gencods = extract_gencods_from_search_response(
            search_response, from_date="15/10/2024"
        )

        # Assert - Only article without date should be included
        assert len(gencods) == 1
        assert "9781234567891" in gencods
        assert "9781234567890" not in gencods

    def test_extract_gencods_with_invalid_from_date_format(self):
        """Test error handling for invalid from_date format."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": {"1": {"gencod": "9781234567890", "titre": "Book"}},
                }
            ]
        }

        # Act & Assert
        with pytest.raises(
            ValueError, match="Invalid from_date format.*Expected DD/MM/YYYY"
        ):
            extract_gencods_from_search_response(
                search_response,
                from_date="2024-10-15",  # Wrong format
            )

    def test_extract_gencods_with_invalid_article_date_format(self):
        """Test that articles with invalid datemodification are included with warn."""
        # Arrange
        search_response = {
            "result": [
                {
                    "id": 123,
                    "article": {
                        "1": {
                            "gencod": "9781234567890",
                            "datemodification": "invalid-date",
                        },
                        "2": {
                            "gencod": "9781234567891",
                            "datemodification": "20/10/2024",
                        },
                    },
                }
            ]
        }

        # Act - Filter from 15/10/2024
        gencods = extract_gencods_from_search_response(
            search_response, from_date="15/10/2024"
        )

        # Assert - Both should be included (invalid date treated as "include")
        assert len(gencods) == 2
        assert "9781234567890" in gencods
        assert "9781234567891" in gencods
