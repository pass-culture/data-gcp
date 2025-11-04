"""Tests for API transformation utilities."""

import json

import pandas as pd
import pytest

from src.utils.api_transform import transform_api_response


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
