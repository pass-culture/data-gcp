import importlib

import numpy as np
import pandas as pd
import pytest
from typer.testing import CliRunner

from src.constants import (
    DESCRIPTION_SIMILARITY_COL,
    FULL_DESCRIPTION_SIMILARITY_COL,
    FULL_NAME_SIMILARITY_COL,
    IMAGE_EMBEDDING_COL,
    IMAGE_SIMILARITY_COL,
    NAME_SIMILARITY_COL,
    OFFER_DESCRIPTION_COL,
    OFFER_ID_COL,
    OFFER_NAME_COL,
    OFFER_SUBCATEGORY_ID_COL,
    PARTIAL_NAME_SIMILARITY_COL,
)

# Since the file name starts with a number, we import it using importlib.
compute_similarities_module = importlib.import_module("cli.2_compute_similarities")
compute_similarities = compute_similarities_module.compute_similarities
main_cli = compute_similarities_module.app


class TestComputeSimilarities:
    def test_compute_similarities_with_matching_and_non_matching_offers(self):
        # Prepare mock input dataframe
        # Description length must be >= 30, otherwise it is marked NaN.
        selected_df = pd.DataFrame(
            {
                OFFER_ID_COL: ["1", "2", "3"],
                OFFER_NAME_COL: [
                    "Concert de Johnny Hallyday",
                    "Concert Johnny Hallyday Paris",
                    "Spectacle de Jamel Debbouze au Comedy Club",
                ],
                OFFER_DESCRIPTION_COL: [
                    "Un concert incroyable de Johnny Hallyday.",
                    "Un spectacle de Johnny Hallyday à Paris.",
                    "Un spectacle humoristique par Jamel.",
                ],
                IMAGE_EMBEDDING_COL: [
                    np.array([1.0, 0.0], dtype=np.float32),
                    np.array([1.0, 0.0], dtype=np.float32),
                    np.array([0.0, 1.0], dtype=np.float32),
                ],
            }
        )

        # Execute compute_similarities
        similarities_df = compute_similarities(selected_df)

        # Assertions
        assert isinstance(similarities_df, pd.DataFrame)
        # We expect only the pair ("2", "1") to pass name similarity.
        # "Spectacle de Jamel..." (3) has low partial name similarity ratio,
        # so it should be filtered out.
        assert len(similarities_df) == 1

        row = similarities_df.iloc[0]
        assert row[f"{OFFER_ID_COL}_1"] == "2"
        assert row[f"{OFFER_ID_COL}_2"] == "1"

        assert f"{OFFER_NAME_COL}_1" in similarities_df.columns
        assert f"{OFFER_NAME_COL}_2" in similarities_df.columns
        assert f"{OFFER_DESCRIPTION_COL}_1" in similarities_df.columns
        assert f"{OFFER_DESCRIPTION_COL}_2" in similarities_df.columns

        assert row[f"{OFFER_NAME_COL}_1"] == "Concert Johnny Hallyday Paris"
        assert row[f"{OFFER_NAME_COL}_2"] == "Concert de Johnny Hallyday"

        # Verify that all similarity metrics are present and have expected properties
        assert NAME_SIMILARITY_COL in similarities_df.columns
        assert PARTIAL_NAME_SIMILARITY_COL in similarities_df.columns
        assert FULL_NAME_SIMILARITY_COL in similarities_df.columns
        assert IMAGE_SIMILARITY_COL in similarities_df.columns
        assert DESCRIPTION_SIMILARITY_COL in similarities_df.columns
        assert FULL_DESCRIPTION_SIMILARITY_COL in similarities_df.columns

        # Since [1.0, 0.0] @ [1.0, 0.0] = 1.0
        assert pytest.approx(row[IMAGE_SIMILARITY_COL]) == 1.0

        # Assert rapidfuzz score ranges (0 to 100)
        assert 0 <= row[NAME_SIMILARITY_COL] <= 100
        assert 0 <= row[PARTIAL_NAME_SIMILARITY_COL] <= 100
        assert 0 <= row[FULL_NAME_SIMILARITY_COL] <= 100
        assert 0 <= row[DESCRIPTION_SIMILARITY_COL] <= 100
        assert 0 <= row[FULL_DESCRIPTION_SIMILARITY_COL] <= 100


class TestComputeSimilaritiesCLIIntegration:
    def test_cli_integration_workflow(self, tmp_path):
        # Create input Parquet file with offers under different subcategories
        input_data = pd.DataFrame(
            {
                OFFER_ID_COL: ["1", "2", "3", "4"],
                OFFER_SUBCATEGORY_ID_COL: [
                    "CONCERT",
                    "CONCERT",
                    "CONCERT",
                    "THEATRE",
                ],
                OFFER_NAME_COL: [
                    "Concert de Johnny Hallyday",
                    "Concert Johnny Hallyday Paris",
                    "Spectacle de Jamel Debbouze au Comedy Club",  # Low sim
                    "Une piece de theatre de Moliere",  # Different subcategory
                ],
                OFFER_DESCRIPTION_COL: [
                    "Un incroyable concert de Johnny Hallyday.",
                    "Un spectacle de Johnny Hallyday à Paris.",
                    "Un spectacle humoristique par Jamel.",
                    "Une piece de theatre de Moliere clas.",
                ],
                IMAGE_EMBEDDING_COL: [
                    [1.0, 0.0],
                    [1.0, 0.0],
                    [0.0, 1.0],
                    [0.5, 0.5],
                ],
            }
        )

        input_filepath = tmp_path / "offers_with_embeddings.parquet"
        output_filepath = tmp_path / "output_similarities.parquet"

        # Write to parquet
        input_data.to_parquet(input_filepath)

        # Run typer CLI target command
        runner = CliRunner()
        result = runner.invoke(
            main_cli,
            [
                "--offer-event-with-embeddings-filepath",
                str(input_filepath),
                "--output-filepath",
                str(output_filepath),
            ],
        )

        # CLI should finish successfully
        assert result.exit_code == 0, f"CLI output: {result.output}"

        # Output parquet file should exist and contain correct matching pairs
        assert output_filepath.exists()
        similarities_df = pd.read_parquet(output_filepath)

        # We expect only the MATCHING of the CONCERT category to pass.
        # Theatre has no other pairs in its subcategory, and Offer 3 has
        # low partial name similarity with Offer 1 and 2.
        assert len(similarities_df) == 1

        row = similarities_df.iloc[0]
        assert row[f"{OFFER_ID_COL}_1"] == "2"
        assert row[f"{OFFER_ID_COL}_2"] == "1"
        assert row[f"{OFFER_NAME_COL}_1"] == "Concert Johnny Hallyday Paris"
        assert row[f"{OFFER_NAME_COL}_2"] == "Concert de Johnny Hallyday"
        assert row[IMAGE_SIMILARITY_COL] == 1.0
