"""Integration tests for the complete ETL workflow."""

import json
import math
import tempfile
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
import typer.testing

# from scripts.extract_new_offers_from_titelive import app as extract_app
# from scripts.parse_offers import app as parse_app
# from src.constants import TITELIVE_CATEGORIES


class TestETLWorkflow:
    """Integration tests for the complete ETL workflow."""

    def setup_method(self):
        """Set up test fixtures for each test method."""
        self.runner = typer.testing.CliRunner()
        self.temp_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """Clean up after each test method."""
        # Clear any module imports to avoid cross-test pollution
        import sys

        modules_to_clear = [
            module for module in sys.modules if module.startswith("scripts.")
        ]
        for module in modules_to_clear:
            if module in sys.modules:
                del sys.modules[module]

    @pytest.fixture()
    def sample_book_data(self):
        """Fixture providing consistent sample book data for tests."""
        return {
            "id": "book1",
            "titre": "Test Book",
            "auteurs_multi": [{"nom": "Test Author", "prenom": "John"}],
            "article": {
                "art1": {
                    "ean": "9781234567890",
                    "prix": 15.99,
                    "datemodification": "01/01/2024",
                    "taux_tva": 5.5,
                    "image": 1,
                    "iad": 0,
                    "typeproduit": 1,
                }
            },
        }

    def test_extract_script_integration(self, sample_book_data):
        """Test the extract script with mocked API responses."""

        with patch("src.utils.requests.get_modified_offers") as mock_get_offers:
            from scripts.extract_new_offers_from_titelive import app as extract_app

            # Arrange - Use the same data structure as the complete workflow test
            mock_data = [
                {
                    "id": "book1",
                    "titre": "Test Book 1",
                    "auteurs_multi": [{"nom": "Author1", "prenom": "John"}],
                    "article": {
                        "art1": {
                            "ean": "9781234567890",
                            "prix": 15.99,
                            "datemodification": "01/01/2024",
                            "taux_tva": 5.5,
                            "image": 1,
                            "iad": 0,
                            "typeproduit": 1,
                        }
                    },
                },
                {
                    "id": "book2",
                    "titre": "Test Book 2",
                    "auteurs_multi": [{"nom": "Author2", "prenom": "Jane"}],
                    "article": {
                        "art2": {
                            "ean": "9781234567891",
                            "prix": 12.50,
                            "datemodification": "02/01/2024",
                            "taux_tva": 5.5,
                            "image": 0,
                            "iad": 1,
                            "typeproduit": 2,
                        }
                    },
                },
            ]

            mock_df = pd.DataFrame(
                {
                    "id": [data["id"] for data in mock_data],
                    "data": [json.dumps(data) for data in mock_data],
                }
            ).set_index("id")
            mock_get_offers.return_value = mock_df

            output_file = Path(self.temp_dir) / "test_extract.parquet"

            # Act
            result = self.runner.invoke(
                extract_app,
                [
                    "--offer-category",
                    "paper",
                    "--min-modified-date",
                    "2024-01-01",
                    "--output-file-path",
                    str(output_file),
                ],
            )

            # Assert
            assert result.exit_code == 0
            assert output_file.exists()

            # Verify the output file content
            saved_df = pd.read_parquet(output_file)
            assert len(saved_df) == 2
            assert "data" in saved_df.columns
            # Verify the mock was called
            assert mock_get_offers.call_count >= 1

    def test_parse_script_integration(self):
        """Test the parse script with real data processing."""
        # Import here to avoid GCP constant issues
        from scripts.parse_offers import app as parse_app

        # Arrange
        input_data = pd.DataFrame(
            {
                "data": [
                    json.dumps(
                        {
                            "id": "book1",
                            "titre": "Test Book 1",
                            "auteurs_multi": [{"nom": "Author1", "prenom": "John"}],
                            "article": {
                                "art1": {
                                    "ean": "9781234567890",
                                    "prix": 15.99,
                                    "datemodification": "01/01/2024",
                                    "taux_tva": 5.5,
                                    "image": 1,
                                    "iad": 0,
                                    "typeproduit": 1,
                                }
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "id": "book2",
                            "titre": "Test Book 2",
                            "auteurs_multi": [{"nom": "Author2", "prenom": "Jane"}],
                            "article": {
                                "art2": {
                                    "ean": "9781234567891",
                                    "prix": 12.50,
                                    "datemodification": "02/01/2024",
                                    "taux_tva": 5.5,
                                    "image": 0,
                                    "iad": 1,
                                    "typeproduit": 2,
                                }
                            },
                        }
                    ),
                ]
            }
        )

        input_file = Path(self.temp_dir) / "test_input.parquet"
        output_file = Path(self.temp_dir) / "test_output.parquet"
        input_data.to_parquet(input_file)

        # Act
        result = self.runner.invoke(
            parse_app,
            [
                "--min-modified-date",
                "2024-01-01",
                "--input-file-path",
                str(input_file),
                "--output-file-path",
                str(output_file),
            ],
        )

        # Assert
        assert result.exit_code == 0
        assert output_file.exists()

        # Verify the output file content
        saved_df = pd.read_parquet(output_file)
        assert len(saved_df) == 2
        assert "titre" in saved_df.columns
        assert "article_ean" in saved_df.columns
        assert "auteurs_multi" in saved_df.columns

        # Check specific values
        assert saved_df["titre"].iloc[0] == "Test Book 1"
        assert saved_df["article_ean"].iloc[0] == "9781234567890"
        assert math.isclose(saved_df["article_prix"].iloc[0], 15.99)

        # Check that auteurs_multi is JSON string
        auteurs_data = json.loads(saved_df["auteurs_multi"].iloc[0])
        assert auteurs_data[0]["nom"] == "Author1"

    def test_complete_workflow_integration(self):
        """Test the complete workflow from extraction to parsing."""

        with patch("src.utils.requests.get_modified_offers") as mock_get_offers:
            # Import here to avoid GCP constant issues
            from scripts.extract_new_offers_from_titelive import app as extract_app
            from scripts.parse_offers import app as parse_app

            # Arrange
            raw_data = {
                "id": "book1",
                "titre": "Integration Test Book",
                "auteurs_multi": [{"nom": "Test Author", "prenom": "Integration"}],
                "article": {
                    "art1": {
                        "ean": "9781234567890",
                        "prix": 25.99,
                        "datemodification": "15/01/2024",
                        "taux_tva": 5.5,
                        "image": 1,
                        "iad": 0,
                        "typeproduit": 1,
                    }
                },
            }

            mock_df = pd.DataFrame(
                {
                    "id": ["book1"],
                    "data": [json.dumps(raw_data)],
                }
            ).set_index("id")
            mock_get_offers.return_value = mock_df

            extract_output = Path(self.temp_dir) / "extracted.parquet"
            parse_output = Path(self.temp_dir) / "parsed.parquet"

            # Act - Step 1: Extract
            extract_result = self.runner.invoke(
                extract_app,
                [
                    "--offer-category",
                    "paper",
                    "--min-modified-date",
                    "2024-01-01",
                    "--output-file-path",
                    str(extract_output),
                ],
            )

            # Act - Step 2: Parse
            parse_result = self.runner.invoke(
                parse_app,
                [
                    "--min-modified-date",
                    "2024-01-01",
                    "--input-file-path",
                    str(extract_output),
                    "--output-file-path",
                    str(parse_output),
                ],
            )

            # Assert
            assert extract_result.exit_code == 0
            assert parse_result.exit_code == 0
            assert extract_output.exists()
            assert parse_output.exists()

            # Verify final output
            final_df = pd.read_parquet(parse_output)
            assert len(final_df) == 1
            assert final_df["titre"].iloc[0] == "Integration Test Book"
            assert final_df["article_ean"].iloc[0] == "9781234567890"
            assert math.isclose(final_df["article_prix"].iloc[0], 25.99)

            # Check that complex data types are properly handled
            assert isinstance(final_df["auteurs_multi"].iloc[0], str)
            auteurs = json.loads(final_df["auteurs_multi"].iloc[0])
            assert auteurs[0]["prenom"] == "Integration"

    def test_parse_script_date_filtering(self):
        """Test that the parse script correctly filters by modification date."""
        # Import here to avoid GCP constant issues
        from scripts.parse_offers import app as parse_app

        # Arrange - Create data with different modification dates
        input_data = pd.DataFrame(
            {
                "data": [
                    json.dumps(
                        {
                            "id": "old_book",
                            "titre": "Old Book",
                            "auteurs_multi": [],
                            "article": {
                                "art1": {
                                    "ean": "9781111111111",
                                    "datemodification": "01/12/2023",  # Before date
                                }
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "id": "new_book",
                            "titre": "New Book",
                            "auteurs_multi": [],
                            "article": {
                                "art2": {
                                    "ean": "9782222222222",
                                    "datemodification": "15/01/2024",  # After date
                                }
                            },
                        }
                    ),
                    json.dumps(
                        {
                            "id": "no_date_book",
                            "titre": "No Date Book",
                            "auteurs_multi": [],
                            "article": {
                                "art3": {
                                    "ean": "9783333333333",
                                    "datemodification": None,  # No modification date
                                }
                            },
                        }
                    ),
                ]
            }
        )

        input_file = Path(self.temp_dir) / "test_date_filter.parquet"
        output_file = Path(self.temp_dir) / "test_date_output.parquet"
        input_data.to_parquet(input_file)

        # Act
        result = self.runner.invoke(
            parse_app,
            [
                "--min-modified-date",
                "2024-01-01",
                "--input-file-path",
                str(input_file),
                "--output-file-path",
                str(output_file),
            ],
        )

        # Assert
        assert result.exit_code == 0
        saved_df = pd.read_parquet(output_file)

        # Should include new_book (after filter date) and no_date_book (null date)
        # Should exclude old_book (before filter date)
        assert len(saved_df) == 2
        titles = saved_df["titre"].tolist()
        assert "New Book" in titles
        assert "No Date Book" in titles
        assert "Old Book" not in titles

    def test_error_handling_invalid_json(self):
        """Test error handling when input contains invalid JSON."""
        # Import here to avoid GCP constant issues
        from scripts.parse_offers import app as parse_app

        # Arrange
        input_data = pd.DataFrame(
            {
                "data": [
                    "invalid json string",
                    json.dumps(
                        {"id": "valid_book", "titre": "Valid Book", "article": {}}
                    ),
                ]
            }
        )

        input_file = Path(self.temp_dir) / "invalid_json.parquet"
        output_file = Path(self.temp_dir) / "error_output.parquet"
        input_data.to_parquet(input_file)

        # Act
        result = self.runner.invoke(
            parse_app,
            [
                "--min-modified-date",
                "2024-01-01",
                "--input-file-path",
                str(input_file),
                "--output-file-path",
                str(output_file),
            ],
        )

        # Assert - Should fail gracefully or handle invalid JSON
        # Depending on implementation, this might succeed with partial data
        # or fail with a clear error message
        if result.exit_code != 0:
            # Should fail with JSONDecodeError
            assert result.exception is not None
            assert "JSONDecodeError" in str(type(result.exception))
