"""Tests for the parse_offers script functions."""

import pandas as pd

from scripts.parse_offers import post_process_before_saving


class TestPostProcessBeforeSaving:
    """Test cases for post_process_before_saving function."""

    def test_post_process_basic_cleaning(self):
        """Test basic null value cleaning."""
        # Arrange
        df = pd.DataFrame(
            {
                "column1": ["value1", "None", "value2", "nan", None],
                "column2": ["NaN", "value3", None, "value4", "value5"],
            }
        )

        # Act
        result = post_process_before_saving(df)

        # Assert
        expected_column1 = ["value1", None, "value2", None, None]
        expected_column2 = [None, "value3", None, "value4", "value5"]

        assert result["column1"].tolist() == expected_column1
        assert result["column2"].tolist() == expected_column2

    def test_post_process_dict_to_json(self):
        """Test conversion of dictionary columns to JSON."""
        # Arrange
        df = pd.DataFrame(
            {
                "dict_column": [
                    {"key1": "value1", "key2": "value2"},
                    {"key3": "value3"},
                    None,
                    "not_a_dict",
                ],
                "list_column": [
                    ["item1", "item2"],
                    ["item3"],
                    None,
                    "not_a_list",
                ],
                "regular_column": ["value1", "value2", "value3", "value4"],
            }
        )

        # Act
        result = post_process_before_saving(df)

        # Assert
        # Dict column should be converted to JSON
        assert result["dict_column"].iloc[0] == '{"key1": "value1", "key2": "value2"}'
        assert result["dict_column"].iloc[1] == '{"key3": "value3"}'
        assert result["dict_column"].iloc[2] == "null"  # JSON representation of None
        # String gets JSON encoded
        assert result["dict_column"].iloc[3] == '"not_a_dict"'

        # List column should be converted to JSON
        assert result["list_column"].iloc[0] == '["item1", "item2"]'
        assert result["list_column"].iloc[1] == '["item3"]'
        assert result["list_column"].iloc[2] == "null"  # JSON representation of None
        # String gets JSON encoded
        assert result["list_column"].iloc[3] == '"not_a_list"'

        # Regular column should remain unchanged
        expected_regular = ["value1", "value2", "value3", "value4"]
        assert result["regular_column"].tolist() == expected_regular

    def test_post_process_enforce_column_types(self):
        """Test enforcement of specific column types."""
        # Arrange
        df = pd.DataFrame(
            {
                "article_taux_tva": [5.5, 10.0, None, 20.0],
                "article_image": [1, 0, None, 1],
                "article_iad": [0, 1, None, 0],
                "article_typeproduit": [1, 2, None, 3],
                "other_column": ["value1", "value2", "value3", "value4"],
            }
        )

        # Act
        result = post_process_before_saving(df)

        # Assert
        # Check that data types are enforced where possible
        assert result["article_taux_tva"].dtype == float
        assert result["article_image"].dtype == float  # May be float due to NaN
        assert result["article_iad"].dtype == float  # May be float due to NaN
        assert result["article_typeproduit"].dtype == float  # May be float due to NaN

        # Check values (non-null values should be preserved)
        assert result["article_taux_tva"].iloc[0] == 5.5
        assert result["article_image"].iloc[0] == 1.0
        assert result["article_iad"].iloc[1] == 1.0
        assert result["article_typeproduit"].iloc[3] == 3.0

    def test_post_process_complex_scenario(self):
        """Test complex scenario with multiple transformations."""
        # Arrange
        df = pd.DataFrame(
            {
                "titre": ["Book Title", "None", "Another Book"],
                "auteurs_multi": [
                    {"nom": "Author", "prenom": "John"},
                    None,
                    [{"nom": "Author2", "prenom": "Jane"}],
                ],
                "article_taux_tva": [5.5, None, 10.0],
                "article_image": [1, 0, None],
                "regular_column": ["value1", "value2", "value3"],
            }
        )

        # Act
        result = post_process_before_saving(df)

        # Assert
        # Check null cleaning
        assert result["titre"].iloc[1] is None

        # Check JSON conversion
        assert result["auteurs_multi"].iloc[0] == '{"nom": "Author", "prenom": "John"}'
        assert result["auteurs_multi"].iloc[1] == "null"
        assert '"nom": "Author2"' in result["auteurs_multi"].iloc[2]

        # Check type enforcement
        assert result["article_taux_tva"].dtype == float
        assert result["article_image"].dtype == float  # May be float due to NaN
        assert result["article_taux_tva"].iloc[0] == 5.5
        assert pd.isna(result["article_taux_tva"].iloc[1])

    def test_post_process_empty_dataframe(self):
        """Test post-processing of empty DataFrame."""
        # Arrange
        df = pd.DataFrame()

        # Act
        result = post_process_before_saving(df)

        # Assert
        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_post_process_only_enforced_columns(self):
        """Test DataFrame with only columns that have enforced types."""
        # Arrange
        df = pd.DataFrame(
            {
                "article_taux_tva": [5.5, 10.0],
                "article_image": [1, 0],
            }
        )

        # Act
        result = post_process_before_saving(df)

        # Assert
        assert result["article_taux_tva"].dtype == float
        assert result["article_image"].dtype == int
        assert len(result) == 2
