"""Tests for preprocessing utilities."""

from __future__ import annotations

import pandas as pd

from src.utils.preprocessing import (
    detach_single_occuring_metadata,
    normalize_dataframe,
    normalize_gtl,
    remove_rows_with_no_metadata,
)


class TestNormalizeDataframe:
    """Test suite for normalize_dataframe function."""

    def test_normalize_strips_whitespace(self) -> None:
        """Test that whitespace is stripped from string values."""
        df = pd.DataFrame(
            {
                "col1": ["  value1  ", "value2   ", "   value3"],
                "col2": [1, 2, 3],
            }
        )

        result = normalize_dataframe(df, columns=["col1"])

        assert result["col1"].tolist() == ["value1", "value2", "value3"]

    def test_normalize_converts_to_string(self) -> None:
        """Test that values are converted to strings."""
        df = pd.DataFrame(
            {
                "col1": [123, 456.78, True],
                "col2": ["keep", "this", "unchanged"],
            }
        )

        result = normalize_dataframe(df, columns=["col1"])

        assert result["col1"].tolist() == ["123", "456.78", "True"]
        # col2 should be unchanged since it wasn't specified
        assert result["col2"].tolist() == ["keep", "this", "unchanged"]

    def test_normalize_replaces_empty_strings_with_none(self) -> None:
        """Test that empty strings are replaced with None."""
        df = pd.DataFrame(
            {
                "col1": ["value", "", "  ", "another"],
                "col2": [1, 2, 3, 4],
            }
        )

        result = normalize_dataframe(df, columns=["col1"])

        assert result["col1"].tolist() == ["value", None, None, "another"]

    def test_normalize_replaces_missing_value_representations(self) -> None:
        """Test that various missing value representations are replaced with None."""
        df = pd.DataFrame(
            {
                "col1": ["value", "nan", "None", "<NA>", "good"],
            }
        )

        result = normalize_dataframe(df, columns=["col1"])

        assert result["col1"].tolist() == ["value", None, None, None, "good"]

    def test_normalize_handles_actual_nan_values(self) -> None:
        """Test that actual NaN values are handled correctly."""
        df = pd.DataFrame(
            {
                "col1": ["value", None, pd.NA, "another"],
            }
        )

        result = normalize_dataframe(df, columns=["col1"])

        # None and pd.NA should be converted to string "nan" then replaced with None
        assert result["col1"].iloc[0] == "value"
        assert result["col1"].iloc[1] is None
        assert result["col1"].iloc[2] is None
        assert result["col1"].iloc[3] == "another"

    def test_normalize_multiple_columns(self) -> None:
        """Test normalizing multiple columns at once."""
        df = pd.DataFrame(
            {
                "col1": ["  value1  ", "", "value3"],
                "col2": ["  value4", "None", "  "],
                "col3": ["untouched", "data", "here"],
            }
        )

        result = normalize_dataframe(df, columns=["col1", "col2"])

        assert result["col1"].tolist() == ["value1", None, "value3"]
        assert result["col2"].tolist() == ["value4", None, None]
        # col3 should remain unchanged
        assert result["col3"].tolist() == ["untouched", "data", "here"]

    def test_normalize_handles_missing_columns(self) -> None:
        """Test that missing columns are skipped without error."""
        df = pd.DataFrame(
            {
                "col1": ["value1", "value2"],
            }
        )

        # Should not raise an error
        result = normalize_dataframe(df, columns=["col1", "col2", "col3"])

        assert result["col1"].tolist() == ["value1", "value2"]

    def test_normalize_returns_copy(self) -> None:
        """Test that the original dataframe is not modified."""
        df = pd.DataFrame(
            {
                "col1": ["  value  "],
            }
        )

        result = normalize_dataframe(df, columns=["col1"])

        # Original should be unchanged
        assert df["col1"].iloc[0] == "  value  "
        # Result should be normalized
        assert result["col1"].iloc[0] == "value"


class TestDetachSingleOccuringMetadata:
    """Test suite for detach_single_occuring_metadata function."""

    def test_detach_removes_singleton_values(self) -> None:
        """Test that values occurring only once are set to None."""
        df = pd.DataFrame(
            {
                "metadata": ["A", "B", "A", "C"],  # B and C are singletons
                "other": [1, 2, 3, 4],
            }
        )

        result = detach_single_occuring_metadata(df, columns=["metadata"])

        assert result["metadata"].tolist() == ["A", None, "A", None]

    def test_detach_preserves_multiple_occurrences(self) -> None:
        """Test that values occurring multiple times are preserved."""
        df = pd.DataFrame(
            {
                "metadata": ["A", "B", "A", "B", "C"],
            }
        )

        result = detach_single_occuring_metadata(df, columns=["metadata"])

        # A and B appear twice, C appears once
        assert result["metadata"].tolist() == ["A", "B", "A", "B", None]

    def test_detach_handles_multiple_columns(self) -> None:
        """Test detaching singletons from multiple columns."""
        df = pd.DataFrame(
            {
                "col1": ["A", "B", "A"],  # B is singleton
                "col2": ["X", "Y", "Z"],  # All are singletons
                "col3": ["M", "M", "M"],  # All appear 3 times
            }
        )

        result = detach_single_occuring_metadata(df, columns=["col1", "col2", "col3"])

        assert result["col1"].tolist() == ["A", None, "A"]
        assert result["col2"].tolist() == [None, None, None]
        assert result["col3"].tolist() == ["M", "M", "M"]

    def test_detach_handles_none_values(self) -> None:
        """Test that None values are handled correctly."""
        df = pd.DataFrame(
            {
                "metadata": ["A", None, "A", None, "B"],
            }
        )

        result = detach_single_occuring_metadata(df, columns=["metadata"])

        # A appears twice (preserved), B once (removed), None appears twice (preserved)
        assert result["metadata"].tolist()[0] == "A"
        assert pd.isna(result["metadata"].iloc[1])
        assert result["metadata"].tolist()[2] == "A"
        assert pd.isna(result["metadata"].iloc[3])
        assert result["metadata"].tolist()[4] is None

    def test_detach_handles_missing_columns(self) -> None:
        """Test that missing columns are skipped without error."""
        df = pd.DataFrame(
            {
                "col1": ["A", "B", "A"],
            }
        )

        # Should not raise an error
        result = detach_single_occuring_metadata(df, columns=["col1", "col2"])

        assert result["col1"].tolist() == ["A", None, "A"]

    def test_detach_returns_copy(self) -> None:
        """Test that the original dataframe is not modified."""
        df = pd.DataFrame(
            {
                "metadata": ["A", "B", "A"],
            }
        )

        result = detach_single_occuring_metadata(df, columns=["metadata"])

        # Original should be unchanged
        assert df["metadata"].tolist() == ["A", "B", "A"]
        # Result should have singleton removed
        assert result["metadata"].tolist() == ["A", None, "A"]

    def test_detach_empty_dataframe(self) -> None:
        """Test handling of empty dataframe."""
        df = pd.DataFrame({"metadata": []})

        result = detach_single_occuring_metadata(df, columns=["metadata"])

        assert len(result) == 0

    def test_detach_all_unique_values(self) -> None:
        """Test when all values are unique (all should be removed)."""
        df = pd.DataFrame(
            {
                "metadata": ["A", "B", "C", "D"],
            }
        )

        result = detach_single_occuring_metadata(df, columns=["metadata"])

        assert result["metadata"].tolist() == [None, None, None, None]

    def test_detach_preserves_other_columns(self) -> None:
        """Test that columns not specified are left unchanged."""
        df = pd.DataFrame(
            {
                "metadata": ["A", "B", "A"],
                "other": ["X", "Y", "X"],
            }
        )

        result = detach_single_occuring_metadata(df, columns=["metadata"])

        # metadata should be processed
        assert result["metadata"].tolist() == ["A", None, "A"]
        # other should be unchanged
        assert result["other"].tolist() == ["X", "Y", "X"]


class TestRemoveRowsWithNoMetadata:
    """Test suite for remove_rows_with_no_metadata function."""

    def test_remove_rows_all_null(self) -> None:
        """Test that rows with all null metadata are removed."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "meta1": ["A", None, "C", None],
                "meta2": ["X", None, "Z", None],
            }
        )

        result = remove_rows_with_no_metadata(df, metadata_list=["meta1", "meta2"])

        # Rows 2 and 4 have all nulls
        assert len(result) == 2
        assert result["id"].tolist() == [1, 3]

    def test_remove_keeps_rows_with_any_metadata(self) -> None:
        """Test that rows with at least one non-null metadata are kept."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "meta1": ["A", None, "C", None],
                "meta2": [None, "Y", None, None],
            }
        )

        result = remove_rows_with_no_metadata(df, metadata_list=["meta1", "meta2"])

        # All rows except 4 have at least one non-null
        assert len(result) == 3
        assert result["id"].tolist() == [1, 2, 3]

    def test_remove_handles_empty_metadata_list(self) -> None:
        """Test that empty metadata list returns unchanged dataframe."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "meta1": [None, None, None],
            }
        )

        result = remove_rows_with_no_metadata(df, metadata_list=[])

        assert len(result) == 3
        assert result.equals(df)

    def test_remove_handles_none_metadata_list(self) -> None:
        """Test that None metadata list returns unchanged dataframe."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "meta1": [None, None, None],
            }
        )

        result = remove_rows_with_no_metadata(df, metadata_list=None)

        assert len(result) == 3
        assert result.equals(df)

    def test_remove_preserves_all_columns(self) -> None:
        """Test that all columns are preserved in the result."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "meta1": ["A", None, None],
                "meta2": [None, "Y", None],
                "other": ["X", "Y", "Z"],
            }
        )

        result = remove_rows_with_no_metadata(df, metadata_list=["meta1", "meta2"])

        # Row 3 should be removed (all metadata null)
        assert len(result) == 2
        assert list(result.columns) == ["id", "meta1", "meta2", "other"]
        assert result["other"].tolist() == ["X", "Y"]

    def test_remove_returns_copy(self) -> None:
        """Test that the original dataframe is not modified."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "meta1": ["A", None, None],
                "meta2": [None, None, None],
            }
        )

        result = remove_rows_with_no_metadata(df, metadata_list=["meta1", "meta2"])

        # Original should be unchanged
        assert len(df) == 3
        # Result should have row 2 and 3 removed
        assert len(result) == 1

    def test_remove_empty_dataframe(self) -> None:
        """Test handling of empty dataframe."""
        df = pd.DataFrame({"id": [], "meta1": [], "meta2": []})

        result = remove_rows_with_no_metadata(df, metadata_list=["meta1", "meta2"])

        assert len(result) == 0
        assert list(result.columns) == ["id", "meta1", "meta2"]

    def test_remove_all_rows_have_metadata(self) -> None:
        """Test when all rows have at least one metadata value."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "meta1": ["A", None, "C"],
                "meta2": [None, "Y", None],
            }
        )

        result = remove_rows_with_no_metadata(df, metadata_list=["meta1", "meta2"])

        # All rows should be kept
        assert len(result) == 3
        assert result["id"].tolist() == [1, 2, 3]

    def test_remove_single_metadata_column(self) -> None:
        """Test with single metadata column."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "meta1": ["A", None, "C", None],
            }
        )

        result = remove_rows_with_no_metadata(df, metadata_list=["meta1"])

        # Rows 2 and 4 have null in the only metadata column
        assert len(result) == 2
        assert result["id"].tolist() == [1, 3]

    def test_remove_subset_of_columns(self) -> None:
        """Test that only specified metadata columns are checked."""
        df = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "meta1": [None, None, None],
                "meta2": ["X", "Y", "Z"],
            }
        )

        # Only check meta1 (all null, so all rows removed)
        result = remove_rows_with_no_metadata(df, metadata_list=["meta1"])

        assert len(result) == 0

        # Check both (meta2 has values, so all rows kept)
        result = remove_rows_with_no_metadata(df, metadata_list=["meta1", "meta2"])

        assert len(result) == 3


class TestPreprocessingPipeline:
    """Integration tests for preprocessing pipeline."""

    def test_full_pipeline(self) -> None:
        """Test the full preprocessing pipeline used in graph builders."""
        df = pd.DataFrame(
            {
                "item_id": ["book-1", "book-2", "book-3", "book-4", "book-5"],
                "metadata1": ["  A  ", "B", "A", "", "C"],
                "metadata2": ["X", "Y", "X", "None", "Y"],
            }
        )

        # Apply full pipeline
        result = (
            df.pipe(normalize_dataframe, columns=["metadata1", "metadata2"])
            .pipe(detach_single_occuring_metadata, columns=["metadata1", "metadata2"])
            .pipe(
                remove_rows_with_no_metadata,
                metadata_list=["metadata1", "metadata2"],
            )
        )

        # After normalization: metadata1 = ["A", "B", "A", None, "C"]
        #                     metadata2 = ["X", "Y", "X", None, "Y"]
        # After detach singletons: metadata1 = ["A", None, "A", None, None]
        #                          metadata2 = ["X", "Y", "X", None, "Y"]
        #                          (B and C are singletons in metadata1)
        # After removing rows with no metadata: keep rows 1, 2, 3, 5
        assert len(result) == 4
        assert result["item_id"].tolist() == ["book-1", "book-2", "book-3", "book-5"]

    def test_pipeline_with_real_book_data_structure(self) -> None:
        """Test pipeline with structure similar to real book data."""
        df = pd.DataFrame(
            {
                "item_id": ["book-1", "book-2", "book-3", "book-4"],
                "artist_id": ["artist-1", "artist-2", "artist-1", "artist-3"],
                "gtl_label_level_1": ["Arts", "Comics", "Arts", "   "],
                "gtl_label_level_2": ["Painting", "", "Painting", None],
            }
        )

        result = (
            df.pipe(
                normalize_dataframe,
                columns=["artist_id", "gtl_label_level_1", "gtl_label_level_2"],
            )
            .pipe(
                detach_single_occuring_metadata,
                columns=["artist_id", "gtl_label_level_1", "gtl_label_level_2"],
            )
            .pipe(
                remove_rows_with_no_metadata,
                metadata_list=["artist_id", "gtl_label_level_1", "gtl_label_level_2"],
            )
        )

        # artist-1 appears twice, artist-2 and artist-3 are singletons
        # Arts appears twice, Comics is singleton
        # Painting appears twice
        # After processing, book-2 and book-4 should have limited metadata
        assert len(result) > 0
        assert "book-1" in result["item_id"].tolist()
        assert "book-3" in result["item_id"].tolist()


class TestNormalizeGTL:
    """Test suite for normalize_gtl function."""

    def test_normalize_gtl(self) -> None:
        """Test the normalize_gtl function for GTL ID normalization."""
        df = pd.DataFrame(
            {
                "item_id": [
                    "book-1",
                    "book-2",
                    "book-3",
                    "book-4",
                    "book-5",
                    "book-6",
                    "book-7",
                    "book-8",
                    "book-9",
                ],
                "gtl_id": [
                    " 1234567 ",
                    "00012345",
                    "nan",
                    "",
                    "  98765432",
                    "1234500",
                    "123450",
                    "0123456e",
                    "01234 56",
                ],
            }
        )

        normalized_df = normalize_gtl(df)

        expected_gtl_ids = [
            "01234567",
            None,
            None,
            None,
            "98765432",
            "01234500",
            None,
            None,
            None,
        ]

        assert normalized_df["gtl_id"].tolist() == expected_gtl_ids
