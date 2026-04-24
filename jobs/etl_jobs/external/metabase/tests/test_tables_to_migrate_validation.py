"""Tests for TablesToMigrate Pydantic model validation.

Ensures that the tables-to-migrate.json schema is properly validated:
- Valid entries are accepted
- Invalid entries are rejected with clear error messages
- Both target_table and columns_to_migrate can be present or absent individually
- At least one of them must be present
- Key and target_table formats are validated
"""

import pytest
from pydantic import ValidationError

from api.models import TableMigrationEntry, TablesToMigrate


class TestTableMigrationEntry:
    """Test TableMigrationEntry validation."""

    def test_entry_with_both_fields(self) -> None:
        """Valid entry with both target_table and columns_to_migrate."""
        # Given
        data = {
            "target_table": "new_schema.new_table",
            "columns_to_migrate": {"old_col": "new_col"},
        }

        # When
        result = TableMigrationEntry.model_validate(data)

        # Then
        expected = {
            "target_table": "new_schema.new_table",
            "columns_to_migrate": {"old_col": "new_col"},
        }
        assert result.model_dump() == expected, "Entry should accept both fields"

    def test_entry_with_only_target_table(self) -> None:
        """Valid entry with only target_table (table rename only)."""
        # Given
        data = {"target_table": "new_schema.new_table"}

        # When
        result = TableMigrationEntry.model_validate(data)

        # Then
        expected = {
            "target_table": "new_schema.new_table",
            "columns_to_migrate": None,
        }
        assert result.model_dump() == expected, "Entry should accept target_table only"

    def test_entry_with_only_columns(self) -> None:
        """Valid entry with only columns_to_migrate (columns-only migration)."""
        # Given
        data = {"columns_to_migrate": {"old_col": "new_col"}}

        # When
        result = TableMigrationEntry.model_validate(data)

        # Then
        expected = {
            "target_table": None,
            "columns_to_migrate": {"old_col": "new_col"},
        }
        assert result.model_dump() == expected, "Entry should accept columns_to_migrate only"

    def test_entry_with_empty_columns_dict(self) -> None:
        """Entry with empty columns_to_migrate dict is valid at entry level."""
        # Given
        data = {"columns_to_migrate": {}}

        # When
        result = TableMigrationEntry.model_validate(data)

        # Then
        expected = {
            "target_table": None,
            "columns_to_migrate": {},
        }
        assert result.model_dump() == expected, "Entry should accept empty columns dict"

    def test_entry_rejects_extra_fields(self) -> None:
        """Entry with extra fields should be rejected (extra='forbid')."""
        # Given
        data = {
            "target_table": "schema.table",
            "columns_to_migrate": {"old": "new"},
            "extra_field": "should_fail",
        }

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            TableMigrationEntry.model_validate(data)

        assert "extra_field" in str(exc_info.value), "Should reject extra fields"


class TestTablesToMigrate:
    """Test TablesToMigrate root model validation."""

    def test_valid_multi_table_config(self) -> None:
        """Valid configuration with multiple tables."""
        # Given
        data = {
            "analytics.users": {
                "target_table": "new_analytics.users",
                "columns_to_migrate": {"old_col": "new_col"},
            },
            "analytics.orders": {
                "columns_to_migrate": {"order_id": "id", "user_id": "customer_id"},
            },
            "analytics.products": {
                "target_table": "new_analytics.products",
            },
        }

        # When
        result = TablesToMigrate.model_validate(data)

        # Then
        expected = {
            "analytics.users": {
                "target_table": "new_analytics.users",
                "columns_to_migrate": {"old_col": "new_col"},
            },
            "analytics.orders": {
                "target_table": None,
                "columns_to_migrate": {"order_id": "id", "user_id": "customer_id"},
            },
            "analytics.products": {
                "target_table": "new_analytics.products",
                "columns_to_migrate": None,
            },
        }
        assert result.model_dump() == expected, "Should accept valid multi-table config"

    def test_valid_columns_only_migration(self) -> None:
        """Valid entry with only columns_to_migrate (same table name)."""
        # Given
        data = {
            "analytics.users": {
                "columns_to_migrate": {"old_col": "new_col"},
            },
        }

        # When
        result = TablesToMigrate.model_validate(data)

        # Then
        expected = {
            "analytics.users": {
                "target_table": None,
                "columns_to_migrate": {"old_col": "new_col"},
            },
        }
        assert result.model_dump() == expected, "Should accept columns-only migration"

    def test_valid_table_rename_only(self) -> None:
        """Valid entry with only target_table (no column changes)."""
        # Given
        data = {
            "analytics.old_users": {
                "target_table": "analytics.new_users",
            },
        }

        # When
        result = TablesToMigrate.model_validate(data)

        # Then
        expected = {
            "analytics.old_users": {
                "target_table": "analytics.new_users",
                "columns_to_migrate": None,
            },
        }
        assert result.model_dump() == expected, "Should accept table rename only"

    def test_rejects_entry_with_neither_field(self) -> None:
        """Entry with neither target_table nor columns_to_migrate should fail."""
        # Given
        data = {
            "analytics.users": {
                "target_table": None,
                "columns_to_migrate": None,
            },
        }

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            TablesToMigrate.model_validate(data)

        error_msg = str(exc_info.value)
        assert "analytics.users" in error_msg, "Error should mention the key"
        assert "at least one of" in error_msg.lower(), "Error should explain the constraint"

    def test_rejects_entry_with_both_fields_missing(self) -> None:
        """Entry with both fields explicitly missing should fail."""
        # Given
        data = {
            "analytics.users": {},
        }

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            TablesToMigrate.model_validate(data)

        error_msg = str(exc_info.value)
        assert "analytics.users" in error_msg, "Error should mention the key"
        assert "at least one of" in error_msg.lower(), "Error should explain the constraint"

    def test_rejects_key_without_dot(self) -> None:
        """Key without a dot (no schema) should be rejected."""
        # Given
        data = {
            "users": {
                "columns_to_migrate": {"old_col": "new_col"},
            },
        }

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            TablesToMigrate.model_validate(data)

        error_msg = str(exc_info.value)
        assert "users" in error_msg, "Error should mention the invalid key"
        assert "schema.table" in error_msg, "Error should explain format"
        assert "exactly one dot" in error_msg, "Error should explain constraint"

    def test_rejects_key_with_multiple_dots(self) -> None:
        """Key with multiple dots should be rejected."""
        # Given
        data = {
            "analytics.public.users": {
                "columns_to_migrate": {"old_col": "new_col"},
            },
        }

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            TablesToMigrate.model_validate(data)

        error_msg = str(exc_info.value)
        assert "analytics.public.users" in error_msg, "Error should mention the invalid key"
        assert "exactly one dot" in error_msg, "Error should explain constraint"

    def test_rejects_target_table_without_dot(self) -> None:
        """target_table without a dot should be rejected."""
        # Given
        data = {
            "analytics.users": {
                "target_table": "new_users",
            },
        }

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            TablesToMigrate.model_validate(data)

        error_msg = str(exc_info.value)
        assert "new_users" in error_msg, "Error should mention the invalid target_table"
        assert "target_table" in error_msg, "Error should identify the field"
        assert "schema.table" in error_msg, "Error should explain format"

    def test_rejects_target_table_with_multiple_dots(self) -> None:
        """target_table with multiple dots should be rejected."""
        # Given
        data = {
            "analytics.users": {
                "target_table": "new.analytics.users",
            },
        }

        # When/Then
        with pytest.raises(ValidationError) as exc_info:
            TablesToMigrate.model_validate(data)

        error_msg = str(exc_info.value)
        assert "new.analytics.users" in error_msg, "Error should mention the invalid target_table"
        assert "exactly one dot" in error_msg, "Error should explain constraint"

    def test_accepts_empty_columns_dict_with_target_table(self) -> None:
        """Empty columns dict is OK if target_table is present."""
        # Given
        data = {
            "analytics.users": {
                "target_table": "analytics.new_users",
                "columns_to_migrate": {},
            },
        }

        # When
        result = TablesToMigrate.model_validate(data)

        # Then
        expected = {
            "analytics.users": {
                "target_table": "analytics.new_users",
                "columns_to_migrate": {},
            },
        }
        assert result.model_dump() == expected, "Should accept empty columns with target_table"

    def test_complex_column_mappings(self) -> None:
        """Test with realistic complex column mappings."""
        # Given
        data = {
            "analytics_stg.enriched_user_data": {
                "columns_to_migrate": {
                    "user_id": "user_id",
                    "booking_cnt": "total_individual_bookings",
                    "actual_amount_spent": "total_actual_amount_spent",
                    "user_creation_date": "user_creation_date",
                    "user_department_code": "user_department_code",
                },
            },
        }

        # When
        result = TablesToMigrate.model_validate(data)

        # Then
        expected = {
            "analytics_stg.enriched_user_data": {
                "target_table": None,
                "columns_to_migrate": {
                    "user_id": "user_id",
                    "booking_cnt": "total_individual_bookings",
                    "actual_amount_spent": "total_actual_amount_spent",
                    "user_creation_date": "user_creation_date",
                    "user_department_code": "user_department_code",
                },
            },
        }
        assert result.model_dump() == expected, "Should handle complex column mappings"

    def test_schema_names_with_underscores(self) -> None:
        """Schema and table names with underscores should be accepted."""
        # Given
        data = {
            "analytics_stg.firebase_pro_visits": {
                "target_table": "analytics_prod.firebase_professional_visits",
                "columns_to_migrate": {"nb_consult_help": "total_consulted_help"},
            },
        }

        # When
        result = TablesToMigrate.model_validate(data)

        # Then
        expected = {
            "analytics_stg.firebase_pro_visits": {
                "target_table": "analytics_prod.firebase_professional_visits",
                "columns_to_migrate": {"nb_consult_help": "total_consulted_help"},
            },
        }
        assert result.model_dump() == expected, "Should accept underscores in names"

    def test_empty_config(self) -> None:
        """Empty configuration (no tables) should be valid."""
        # Given
        data = {}

        # When
        result = TablesToMigrate.model_validate(data)

        # Then
        expected = {}
        assert result.model_dump() == expected, "Should accept empty config"

    def test_single_table_config(self) -> None:
        """Single table configuration should work."""
        # Given
        data = {
            "analytics.users": {
                "columns_to_migrate": {"old_col": "new_col"},
            },
        }

        # When
        result = TablesToMigrate.model_validate(data)

        # Then
        expected = {
            "analytics.users": {
                "target_table": None,
                "columns_to_migrate": {"old_col": "new_col"},
            },
        }
        assert result.model_dump() == expected, "Should accept single table config"
