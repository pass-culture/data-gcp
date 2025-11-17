"""Tests for file_utils module."""

from datetime import date

import pytest

from utils.file_utils import (
    FileUtilsError,
    compress_directory,
    create_directory_structure,
    get_dated_base_dir,
    safe_fs_name,
    slugify,
    start_of_current_month,
    to_first_of_month,
)


class TestSlugify:
    """Test cases for slugify function."""

    def test_slugify_french_chars(self):
        """Test slugification with French characters."""
        # Arrange & Act
        result = slugify("Île-de-France")

        # Assert
        assert result == "Ile-de-France"

    def test_slugify_special_chars(self):
        """Test slugification with special characters."""
        # Arrange & Act
        result = slugify("Test/Name\\With:Special*Chars")

        # Assert
        assert "/" not in result
        assert "\\" not in result
        assert ":" not in result
        assert "*" not in result

    def test_slugify_preserves_hyphens_underscores(self):
        """Test that hyphens and underscores are preserved."""
        # Arrange & Act
        result = slugify("test-name_with_parts")

        # Assert
        assert result == "test-name_with_parts"

    def test_slugify_empty_string(self):
        """Test slugification of empty string."""
        # Arrange & Act
        result = slugify("")

        # Assert
        assert result == "default"


class TestSafeFsName:
    """Test cases for safe_fs_name function."""

    def test_safe_fs_name_special_chars(self):
        """Test safe filesystem name with special characters."""
        # Arrange & Act
        result = safe_fs_name('Test<>:"/\\|?*Name')

        # Assert
        assert "<" not in result
        assert ">" not in result
        assert ":" not in result
        assert '"' not in result
        assert "/" not in result
        assert "\\" not in result
        assert "|" not in result
        assert "?" not in result
        assert "*" not in result

    def test_safe_fs_name_preserves_french_chars(self):
        """Test that French characters are preserved."""
        # Arrange & Act
        result = safe_fs_name("Île-de-France")

        # Assert
        assert "Île" in result

    def test_safe_fs_name_max_length(self):
        """Test that name is truncated to 50 characters."""
        # Arrange
        long_name = "A" * 100

        # Act
        result = safe_fs_name(long_name)

        # Assert
        assert len(result) == 50

    def test_safe_fs_name_empty_string(self):
        """Test safe filesystem name with empty string."""
        # Arrange & Act
        result = safe_fs_name("")

        # Assert
        assert result == "default"


class TestStartOfCurrentMonth:
    """Test cases for start_of_current_month function."""

    def test_start_of_current_month_format(self):
        """Test that result is in YYYY-MM-01 format."""
        # Act
        result = start_of_current_month()

        # Assert
        assert isinstance(result, str)
        assert result.endswith("-01")
        parts = result.split("-")
        assert len(parts) == 3
        assert len(parts[0]) == 4  # Year
        assert len(parts[1]) == 2  # Month
        assert parts[2] == "01"  # Day


class TestToFirstOfMonth:
    """Test cases for to_first_of_month function."""

    def test_to_first_of_month_conversion(self):
        """Test conversion to first day of month."""
        # Arrange & Act
        result = to_first_of_month("2024-10-15")

        # Assert
        assert result == "2024-10-01"

    def test_to_first_of_month_already_first(self):
        """Test when date is already first of month."""
        # Arrange & Act
        result = to_first_of_month("2024-10-01")

        # Assert
        assert result == "2024-10-01"

    def test_to_first_of_month_end_of_month(self):
        """Test conversion from end of month."""
        # Arrange & Act
        result = to_first_of_month("2024-12-31")

        # Assert
        assert result == "2024-12-01"


class TestGetDatedBaseDir:
    """Test cases for get_dated_base_dir function."""

    def test_get_dated_base_dir_format(self, tmp_path):
        """Test date-stamped directory format."""
        # Arrange
        base_dir = tmp_path / "reports"
        ds = "2024-10-15"

        # Act
        result = get_dated_base_dir(base_dir, ds)

        # Assert
        assert result.name == "reports_20241015"
        assert result.parent == base_dir

    def test_get_dated_base_dir_no_ds(self, tmp_path):
        """Test with no ds parameter (uses today)."""
        # Arrange
        base_dir = tmp_path / "reports"

        # Act
        result = get_dated_base_dir(base_dir)

        # Assert
        today = date.today()
        expected_name = f"reports_{today.strftime('%Y%m%d')}"
        assert result.name == expected_name

    def test_get_dated_base_dir_different_dates(self, tmp_path):
        """Test with different dates produce different directories."""
        # Arrange
        base_dir = tmp_path / "reports"

        # Act
        result1 = get_dated_base_dir(base_dir, "2024-10-01")
        result2 = get_dated_base_dir(base_dir, "2024-11-01")

        # Assert
        assert result1.name == "reports_20241001"
        assert result2.name == "reports_20241101"
        assert result1 != result2


class TestCreateDirectoryStructure:
    """Test cases for create_directory_structure function."""

    def test_create_directory_structure_national(self, tmp_path):
        """Test creation of national directory structure."""
        # Arrange
        base_dir = tmp_path / "reports"

        # Act
        stats = create_directory_structure(
            base_dir=base_dir, selected_regions=None, national=True
        )

        # Assert
        assert (base_dir / "NATIONAL").exists()
        assert stats["directories_created"] >= 1

    def test_create_directory_structure_regional(self, tmp_path):
        """Test creation of regional directory structure."""
        # Arrange
        base_dir = tmp_path / "reports"
        regions = ["Île-de-France", "Auvergne-Rhône-Alpes"]

        # Act
        stats = create_directory_structure(
            base_dir=base_dir, selected_regions=regions, national=False
        )

        # Assert
        assert (base_dir / "REGIONAL").exists()
        assert (base_dir / "REGIONAL" / "Île-de-France").exists()
        assert (base_dir / "REGIONAL" / "Auvergne-Rhône-Alpes").exists()
        assert stats["directories_created"] >= 3  # REGIONAL + 2 regions

    def test_create_directory_structure_existing(self, tmp_path):
        """Test handling of existing directories."""
        # Arrange
        base_dir = tmp_path / "reports"
        national_dir = base_dir / "NATIONAL"
        national_dir.mkdir(parents=True, exist_ok=True)

        # Act
        stats = create_directory_structure(
            base_dir=base_dir, selected_regions=None, national=True
        )

        # Assert
        assert national_dir.exists()
        assert stats["directories_existing"] >= 1

    def test_create_directory_structure_both_national_and_regional(self, tmp_path):
        """Test creation of both national and regional directories."""
        # Arrange
        base_dir = tmp_path / "reports"
        regions = ["Île-de-France"]

        # Act
        _ = create_directory_structure(
            base_dir=base_dir, selected_regions=regions, national=True
        )

        # Assert
        assert (base_dir / "NATIONAL").exists()
        assert (base_dir / "REGIONAL").exists()
        assert (base_dir / "REGIONAL" / "Île-de-France").exists()


class TestCompressDirectory:
    """Test cases for compress_directory function."""

    def test_compress_directory_success(self, tmp_path):
        """Test successful directory compression."""
        # Arrange
        target_dir = tmp_path / "reports"
        target_dir.mkdir()
        (target_dir / "test.txt").write_text("test content")

        output_dir = tmp_path / "output"

        # Act
        result = compress_directory(
            target_dir=target_dir, output_dir=output_dir, clean_after_compression=False
        )

        # Assert
        assert result == "reports.zip"
        assert (output_dir / "reports.zip").exists()
        assert target_dir.exists()  # Should still exist

    def test_compress_directory_with_cleanup(self, tmp_path):
        """Test compression with cleanup of original directory."""
        # Arrange
        target_dir = tmp_path / "reports"
        target_dir.mkdir()
        (target_dir / "test.txt").write_text("test content")

        output_dir = tmp_path / "output"

        # Act
        result = compress_directory(
            target_dir=target_dir, output_dir=output_dir, clean_after_compression=True
        )

        # Assert
        assert result == "reports.zip"
        assert (output_dir / "reports.zip").exists()
        assert not target_dir.exists()  # Should be deleted

    def test_compress_directory_nonexistent(self, tmp_path):
        """Test error handling when directory doesn't exist."""
        # Arrange
        target_dir = tmp_path / "nonexistent"
        output_dir = tmp_path / "output"

        # Act & Assert
        with pytest.raises(FileUtilsError):
            compress_directory(target_dir=target_dir, output_dir=output_dir)

    def test_compress_directory_creates_output_dir(self, tmp_path):
        """Test that output directory is created if it doesn't exist."""
        # Arrange
        target_dir = tmp_path / "reports"
        target_dir.mkdir()
        (target_dir / "test.txt").write_text("test content")

        output_dir = tmp_path / "output" / "nested"  # Nested non-existent path

        # Act
        compress_directory(target_dir=target_dir, output_dir=output_dir)

        # Assert
        assert output_dir.exists()
        assert (output_dir / "reports.zip").exists()
