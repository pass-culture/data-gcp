"""Unit tests for ContentfulClient."""

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from contentful_client import ContentfulClient


class TestContentfulClient:
    """Test cases for ContentfulClient."""

    @pytest.fixture
    def config_env(self):
        """Provide proper config_env dictionary for ContentfulClient."""
        return {
            "access_token": "test-token",
            "api_url": "cdn.contentful.com",
            "env": "test",
        }

    @pytest.fixture
    def contentful_client(self, config_env, mock_contentful_client):
        """Create a ContentfulClient instance with mocked dependencies."""
        client = ContentfulClient(config_env=config_env)
        client.page_size = 10  # Set a reasonable page size for testing
        return client

    # def test_get_paged_modules_returns_correct_format(
    #     self,
    #     contentful_client,
    #     mock_contentful_client,
    #     sample_module_details,
    #     mock_contentful_entry,
    # ):
    #     """Test that get_paged_modules returns a list of entries with correct format."""
    #     # Setup: Mock entries response
    #     mock_entries_response = MagicMock()
    #     mock_entries_response.total = 3
    #     mock_entries_response.__iter__ = lambda x: iter([mock_contentful_entry] * 3)
    #     mock_entries_response.__len__ = lambda x: 3
    #     mock_contentful_client.entries.return_value = mock_entries_response

    #     # Execute
    #     result = contentful_client.get_paged_modules(sample_module_details)

    #     # Verify output format
    #     assert isinstance(result, list), "Result should be a list"
    #     assert len(result) == 3, "Should return 3 entries"

    #     # Verify each entry has the expected structure
    #     for entry in result:
    #         assert hasattr(entry, "id"), "Each entry should have an id attribute"
    #         assert hasattr(entry, "sys"), "Each entry should have a sys attribute"
    #         assert hasattr(entry, "fields"), "Each entry should have a fields method"
    #         assert entry.id == "test-entry-123", "Entry should have the expected id"

    #     # Verify the method was called with correct content_type
    #     mock_contentful_client.entries.assert_called()
    #     calls = mock_contentful_client.entries.call_args_list

    #     # At least one call should have the correct content_type
    #     content_type_found = False
    #     for call in calls:
    #         query = call[0][0]
    #         if query.get("content_type") == "homepageNatif":
    #             content_type_found = True
    #             break

    #     assert content_type_found, "Should call entries with correct content_type"

    def test_get_basic_fields_returns_correct_format(
        self, contentful_client, mock_contentful_client, mock_contentful_entry
    ):
        """Test that get_basic_fields returns a dictionary with expected fields."""
        # Setup: Mock the HTTP get call for tags
        mock_tag_response = MagicMock()
        mock_tag_response.json.return_value = {"name": "test-tag-name"}
        mock_contentful_client._http_get.return_value = mock_tag_response

        # Execute
        result = contentful_client.get_basic_fields(mock_contentful_entry)

        # Verify output format
        assert isinstance(result, dict), "Result should be a dictionary"

        # Verify required fields are present
        required_fields = [
            "title",
            "date_imported",
            "contentful_tags_id",
            "contentful_tags_name",
            "space",
            "id",
            "type",
            "created_at",
            "updated_at",
            "environment",
            "revision",
            "content_type",
            "locale",
        ]

        for field in required_fields:
            assert field in result, f"Field '{field}' should be present in result"

        # Verify field types and values
        assert result["id"] == "test-entry-123", "ID should match the mock entry"
        assert (
            result["space"] == "test-space-id"
        ), "Space ID should be extracted from sys.space.id"
        assert (
            result["content_type"] == "homepageNatif"
        ), "Content type should be extracted from sys.content_type.id"
        assert (
            result["environment"] == "test"
        ), "Environment should be extracted from sys.environment.id"
        assert result["type"] == "Entry", "Type should be extracted from sys.type"
        assert result["locale"] == "en-US", "Locale should be extracted from sys.locale"
        assert isinstance(
            result["contentful_tags_id"], str
        ), "Tags ID should be a string"
        assert isinstance(
            result["contentful_tags_name"], str
        ), "Tags name should be a string"
        assert (
            result["title"] == "Test Entry Title"
        ), "Title should be extracted from entry.title"

        # Verify datetime field is set
        assert "date_imported" in result, "Should have date_imported field"

        # Verify revision is an integer
        assert result["revision"] == 1, "Revision should be extracted as integer"

    def test_add_other_fields_processes_fields_correctly(self, contentful_client):
        """Test that add_other_fields processes different field types correctly."""
        # Setup: Basic fields dictionary (simulating output from get_basic_fields)
        basic_fields = {
            "id": "test-entry-123",
            "title": "Test Entry",
            "space": "test-space-id",
        }

        # Setup: Other fields with different types to test transformation logic
        mock_recommendation_param = MagicMock()
        mock_recommendation_param.id = "rec-param-123"

        mock_venue_1 = MagicMock()
        mock_venue_1.id = "venue-1"
        mock_venue_2 = MagicMock()
        mock_venue_2.id = "venue-2"

        other_fields = {
            "recommendation_parameters": mock_recommendation_param,
            "venues_search_parameters": [mock_venue_1, mock_venue_2],
            "duration_in_minutes": 120,
            "video_publication_date": "2023-01-01",
            "description": "Test description",
            "unused_field": "This should not be processed",
        }

        # Setup: Module details specifying which fields to process
        module_details = {
            "name": "homepageNatif",
            "additional_fields": [
                "recommendation_parameters",
                "venues_search_parameters",
                "duration_in_minutes",
                "video_publication_date",
                "description",
            ],
        }

        # Execute
        result = contentful_client.add_other_fields(
            basic_fields, other_fields, module_details
        )

        # Verify output format
        assert isinstance(result, dict), "Result should be a dictionary"

        # Verify original basic fields are preserved
        assert (
            result["id"] == "test-entry-123"
        ), "Original basic fields should be preserved"
        assert (
            result["title"] == "Test Entry"
        ), "Original basic fields should be preserved"
        assert (
            result["space"] == "test-space-id"
        ), "Original basic fields should be preserved"

        # Verify field transformations
        assert (
            result["recommendation_parameters"] == "rec-param-123"
        ), "Should extract .id from parameter objects"
        assert (
            result["venues_search_parameters"] == "['venue-1', 'venue-2']"
        ), "Should convert list of IDs to string"
        assert result["duration_in_minutes"] == 120.0, "Should convert to float"
        assert (
            result["video_publication_date"] == "2023-01-01"
        ), "Should preserve date strings"
        assert (
            result["description"] == "Test description"
        ), "Should convert other fields to string"

        # Verify unused field is not processed
        assert (
            "unused_field" not in result
        ), "Fields not in additional_fields should not be processed"

        # Verify the method returns the same dictionary object (modified in place)
        assert (
            result is basic_fields
        ), "Should modify and return the same dictionary object"

    def test_get_all_fields_orchestrates_methods_correctly(
        self,
        contentful_client,
        mock_contentful_client,
        mock_contentful_entry,
        sample_module_details,
    ):
        """Test that get_all_fields properly orchestrates get_basic_fields and add_other_fields."""
        # Setup: Mock the HTTP get call for tags (needed by get_basic_fields)
        mock_tag_response = MagicMock()
        mock_tag_response.json.return_value = {"name": "test-tag-name"}
        mock_contentful_client._http_get.return_value = mock_tag_response

        # Setup: Add additional fields to the mock entry's fields() method
        mock_recommendation_param = MagicMock()
        mock_recommendation_param.id = "rec-param-456"

        # Update the mock entry's fields to include additional fields
        mock_entry_fields = {
            "title": "Test Entry Title",
            "modules": [],
            "description": "Test description",
            "recommendation_parameters": mock_recommendation_param,
            "duration_in_minutes": 90,
        }
        mock_contentful_entry.fields.return_value = mock_entry_fields

        # Setup: Module details with additional fields configuration
        module_details_with_additional = {
            "name": "homepageNatif",
            "additional_fields": [
                "recommendation_parameters",
                "duration_in_minutes",
                "description",
            ],
        }

        # Execute
        result = contentful_client.get_all_fields(
            mock_contentful_entry, module_details_with_additional
        )

        # Verify output format
        assert isinstance(result, dict), "Result should be a dictionary"

        # Verify basic fields are present (from get_basic_fields)
        basic_field_keys = [
            "title",
            "date_imported",
            "contentful_tags_id",
            "contentful_tags_name",
            "space",
            "id",
            "type",
            "created_at",
            "updated_at",
            "environment",
            "revision",
            "content_type",
            "locale",
        ]

        for field in basic_field_keys:
            assert field in result, f"Basic field '{field}' should be present in result"

        # Verify additional fields are processed (from add_other_fields)
        assert (
            "recommendation_parameters" in result
        ), "Additional fields should be processed"
        assert "duration_in_minutes" in result, "Additional fields should be processed"
        assert "description" in result, "Additional fields should be processed"

        # Verify field transformations are applied correctly
        assert (
            result["recommendation_parameters"] == "rec-param-456"
        ), "Should extract .id from parameter objects"
        assert result["duration_in_minutes"] == 90.0, "Should convert to float"
        assert result["description"] == "Test description", "Should convert to string"

        # Verify basic field values are correct
        assert (
            result["id"] == "test-entry-123"
        ), "Basic fields should be extracted correctly"
        assert (
            result["title"] == "Test Entry Title"
        ), "Title should be extracted correctly"
        assert result["space"] == "test-space-id", "Space should be extracted correctly"

        # Verify the method calls the underlying methods correctly
        # (This is implicit - if the result has both basic and additional fields, orchestration worked)

    def test_add_module_infos_to_modules_dataframe_appends_correctly(
        self, contentful_client
    ):
        """Test that add_module_infos_to_modules_dataframe properly appends data to the DataFrame."""
        # Setup: Verify initial state - DataFrame should be empty
        assert len(contentful_client.df_modules) == 0, "DataFrame should start empty"

        # Setup: Sample module info (simulating output from get_all_fields)
        module_info_1 = {
            "id": "entry-123",
            "title": "Test Entry 1",
            "space": "test-space",
            "content_type": "homepageNatif",
            "created_at": "2023-01-01T00:00:00Z",
            "recommendation_parameters": "rec-param-123",
        }

        # Execute: Add first module info
        contentful_client.add_module_infos_to_modules_dataframe(module_info_1)

        # Verify: DataFrame should have one row
        assert (
            len(contentful_client.df_modules) == 1
        ), "DataFrame should have one row after first addition"

        # Verify: Data should be correctly added
        first_row = contentful_client.df_modules.iloc[0]
        assert first_row["id"] == "entry-123", "ID should be correctly added"
        assert first_row["title"] == "Test Entry 1", "Title should be correctly added"
        assert first_row["space"] == "test-space", "Space should be correctly added"
        assert (
            first_row["content_type"] == "homepageNatif"
        ), "Content type should be correctly added"

        # Setup: Second module info to test appending
        module_info_2 = {
            "id": "entry-456",
            "title": "Test Entry 2",
            "space": "test-space",
            "content_type": "algolia",
            "created_at": "2023-01-02T00:00:00Z",
            "description": "Second entry description",
        }

        # Execute: Add second module info
        contentful_client.add_module_infos_to_modules_dataframe(module_info_2)

        # Verify: DataFrame should have two rows
        assert (
            len(contentful_client.df_modules) == 2
        ), "DataFrame should have two rows after second addition"

        # Verify: Both rows should be present with correct data
        assert (
            contentful_client.df_modules.iloc[0]["id"] == "entry-123"
        ), "First row should be preserved"
        assert (
            contentful_client.df_modules.iloc[1]["id"] == "entry-456"
        ), "Second row should be added"
        assert (
            contentful_client.df_modules.iloc[1]["title"] == "Test Entry 2"
        ), "Second row data should be correct"

        # Verify: Index should be properly reset (ignore_index=True)
        expected_index = [0, 1]
        actual_index = contentful_client.df_modules.index.tolist()
        assert actual_index == expected_index, "Index should be properly reset"

        # Verify: DataFrame columns should include all fields from both entries
        expected_columns = {
            "id",
            "title",
            "space",
            "content_type",
            "created_at",
            "recommendation_parameters",
            "description",
        }
        actual_columns = set(contentful_client.df_modules.columns)
        assert expected_columns.issubset(
            actual_columns
        ), "All fields should be present as columns"

    def test_add_parent_child_to_df_manages_relationships_correctly(
        self, contentful_client
    ):
        """Test that add_parent_child_to_df properly manages parent-child relationships."""
        # Setup: Verify initial state - links DataFrame should be empty
        assert (
            len(contentful_client.df_links) == 0
        ), "Links DataFrame should start empty"

        # Verify: Initial DataFrame should have correct columns
        expected_initial_columns = ["parent", "child"]
        actual_initial_columns = contentful_client.df_links.columns.tolist()
        assert (
            actual_initial_columns == expected_initial_columns
        ), "Links DataFrame should have parent and child columns"

        # Execute: Add first parent-child relationship
        parent_id_1 = "homepage-123"
        child_id_1 = "module-456"
        contentful_client.add_parent_child_to_df(parent_id_1, child_id_1)

        # Verify: DataFrame should have one row
        assert (
            len(contentful_client.df_links) == 1
        ), "Links DataFrame should have one row after first addition"

        # Verify: Data should be correctly added
        first_row = contentful_client.df_links.iloc[0]
        assert first_row["parent"] == parent_id_1, "Parent ID should be correctly added"
        assert first_row["child"] == child_id_1, "Child ID should be correctly added"

        # Execute: Add second parent-child relationship
        parent_id_2 = "homepage-123"  # Same parent, different child
        child_id_2 = "module-789"
        contentful_client.add_parent_child_to_df(parent_id_2, child_id_2)

        # Verify: DataFrame should have two rows
        assert (
            len(contentful_client.df_links) == 2
        ), "Links DataFrame should have two rows after second addition"

        # Verify: Both relationships should be present
        assert (
            contentful_client.df_links.iloc[0]["parent"] == parent_id_1
        ), "First relationship should be preserved"
        assert (
            contentful_client.df_links.iloc[0]["child"] == child_id_1
        ), "First relationship should be preserved"
        assert (
            contentful_client.df_links.iloc[1]["parent"] == parent_id_2
        ), "Second relationship should be added"
        assert (
            contentful_client.df_links.iloc[1]["child"] == child_id_2
        ), "Second relationship should be added"

        # Execute: Add third relationship with different parent
        parent_id_3 = "playlist-999"
        child_id_3 = "module-456"  # Same child, different parent
        contentful_client.add_parent_child_to_df(parent_id_3, child_id_3)

        # Verify: DataFrame should have three rows
        assert (
            len(contentful_client.df_links) == 3
        ), "Links DataFrame should have three rows after third addition"

        # Verify: All relationships should be present
        relationships = contentful_client.df_links[["parent", "child"]].values.tolist()
        expected_relationships = [
            [parent_id_1, child_id_1],
            [parent_id_2, child_id_2],
            [parent_id_3, child_id_3],
        ]
        assert (
            relationships == expected_relationships
        ), "All relationships should be correctly stored"

        # Verify: Index should be properly reset (ignore_index=True)
        expected_index = [0, 1, 2]
        actual_index = contentful_client.df_links.index.tolist()
        assert actual_index == expected_index, "Index should be properly reset"

        # Verify: DataFrame structure should remain consistent
        assert list(contentful_client.df_links.columns) == [
            "parent",
            "child",
        ], "Column structure should remain consistent"

    def test_add_tag_to_df_manages_tags_correctly(self, contentful_client):
        """Test that add_tag_to_df properly manages tag data."""
        # Setup: Verify initial state - tags DataFrame should be empty
        assert len(contentful_client.df_tags) == 0, "Tags DataFrame should start empty"

        # Verify: Initial DataFrame should have correct columns
        expected_initial_columns = ["tag_id", "tag_name", "entry_id"]
        actual_initial_columns = contentful_client.df_tags.columns.tolist()
        assert (
            actual_initial_columns == expected_initial_columns
        ), "Tags DataFrame should have tag_id, tag_name, and entry_id columns"

        # Execute: Add first tag
        tag_id_1 = "tag-123"
        tag_name_1 = "culture"
        entry_id_1 = "entry-456"
        contentful_client.add_tag_to_df(tag_id_1, tag_name_1, entry_id_1)

        # Verify: DataFrame should have one row
        assert (
            len(contentful_client.df_tags) == 1
        ), "Tags DataFrame should have one row after first addition"

        # Verify: Data should be correctly added
        first_row = contentful_client.df_tags.iloc[0]
        assert first_row["tag_id"] == tag_id_1, "Tag ID should be correctly added"
        assert first_row["tag_name"] == tag_name_1, "Tag name should be correctly added"
        assert first_row["entry_id"] == entry_id_1, "Entry ID should be correctly added"

        # Execute: Add second tag for same entry
        tag_id_2 = "tag-789"
        tag_name_2 = "music"
        entry_id_2 = "entry-456"  # Same entry, different tag
        contentful_client.add_tag_to_df(tag_id_2, tag_name_2, entry_id_2)

        # Verify: DataFrame should have two rows
        assert (
            len(contentful_client.df_tags) == 2
        ), "Tags DataFrame should have two rows after second addition"

        # Verify: Both tags should be present
        assert (
            contentful_client.df_tags.iloc[0]["tag_id"] == tag_id_1
        ), "First tag should be preserved"
        assert (
            contentful_client.df_tags.iloc[0]["entry_id"] == entry_id_1
        ), "First tag entry should be preserved"
        assert (
            contentful_client.df_tags.iloc[1]["tag_id"] == tag_id_2
        ), "Second tag should be added"
        assert (
            contentful_client.df_tags.iloc[1]["entry_id"] == entry_id_2
        ), "Second tag entry should be added"

        # Execute: Add third tag for different entry
        tag_id_3 = "tag-123"  # Same tag ID, different entry
        tag_name_3 = "culture"  # Same tag name
        entry_id_3 = "entry-999"  # Different entry
        contentful_client.add_tag_to_df(tag_id_3, tag_name_3, entry_id_3)

        # Verify: DataFrame should have three rows
        assert (
            len(contentful_client.df_tags) == 3
        ), "Tags DataFrame should have three rows after third addition"

        # Verify: All tag relationships should be present
        tag_data = contentful_client.df_tags[
            ["tag_id", "tag_name", "entry_id"]
        ].values.tolist()
        expected_tag_data = [
            [tag_id_1, tag_name_1, entry_id_1],
            [tag_id_2, tag_name_2, entry_id_2],
            [tag_id_3, tag_name_3, entry_id_3],
        ]
        assert (
            tag_data == expected_tag_data
        ), "All tag relationships should be correctly stored"

        # Execute: Add fourth tag with special characters
        tag_id_4 = "tag-special-chars"
        tag_name_4 = "événement & culture"
        entry_id_4 = "entry-special"
        contentful_client.add_tag_to_df(tag_id_4, tag_name_4, entry_id_4)

        # Verify: DataFrame should handle special characters
        assert (
            len(contentful_client.df_tags) == 4
        ), "Tags DataFrame should handle special characters"
        fourth_row = contentful_client.df_tags.iloc[3]
        assert (
            fourth_row["tag_name"] == tag_name_4
        ), "Special characters in tag names should be preserved"

        # Verify: Index should be properly reset (ignore_index=True)
        expected_index = [0, 1, 2, 3]
        actual_index = contentful_client.df_tags.index.tolist()
        assert actual_index == expected_index, "Index should be properly reset"

        # Verify: DataFrame structure should remain consistent
        assert list(contentful_client.df_tags.columns) == [
            "tag_id",
            "tag_name",
            "entry_id",
        ], "Column structure should remain consistent"

    def test_set_contentful_modules_filters_correctly(self, contentful_client):
        """Test that set_contentful_modules properly filters modules based on playlist names."""
        # Test 1: Empty playlist names should return all modules
        result_empty = contentful_client.set_contentful_modules([])

        # Verify: Should return all available modules
        assert isinstance(result_empty, list), "Result should be a list"
        assert (
            len(result_empty) > 0
        ), "Should return at least some modules when no filter is applied"

        # Verify: Each module should have the expected structure
        for module in result_empty:
            assert isinstance(module, dict), "Each module should be a dictionary"
            assert "name" in module, "Each module should have a 'name' field"
            assert (
                "additional_fields" in module
            ), "Each module should have 'additional_fields'"
            assert "children" in module, "Each module should have 'children' field"
            assert isinstance(
                module["additional_fields"], list
            ), "additional_fields should be a list"

        # Test 2: Specific playlist names should filter correctly
        playlist_names = ["homepageNatif", "algolia"]
        result_filtered = contentful_client.set_contentful_modules(playlist_names)

        # Verify: Should return only requested modules
        assert isinstance(result_filtered, list), "Filtered result should be a list"
        assert (
            len(result_filtered) == 2
        ), "Should return exactly 2 modules for the specified playlists"

        # Verify: Returned modules should match the requested names
        returned_names = [module["name"] for module in result_filtered]
        assert "homepageNatif" in returned_names, "Should include homepageNatif module"
        assert "algolia" in returned_names, "Should include algolia module"

        # Verify: Should not include other modules
        assert (
            "venuesPlaylist" not in returned_names
        ), "Should not include venuesPlaylist when not requested"

        # Test 3: Single playlist name
        single_playlist = ["homepageNatif"]
        result_single = contentful_client.set_contentful_modules(single_playlist)

        # Verify: Should return only one module
        assert (
            len(result_single) == 1
        ), "Should return exactly 1 module for single playlist"
        assert (
            result_single[0]["name"] == "homepageNatif"
        ), "Should return the correct module"

        # Verify: Module structure should be preserved
        homepage_module = result_single[0]
        assert (
            "additional_fields" in homepage_module
        ), "Module structure should be preserved"
        assert isinstance(
            homepage_module["additional_fields"], list
        ), "additional_fields should remain a list"

        # Test 4: Non-existent playlist name
        non_existent_playlist = ["nonExistentModule"]
        result_non_existent = contentful_client.set_contentful_modules(
            non_existent_playlist
        )

        # Verify: Should return empty list for non-existent modules
        assert isinstance(
            result_non_existent, list
        ), "Result should be a list even for non-existent modules"
        assert (
            len(result_non_existent) == 0
        ), "Should return empty list for non-existent modules"

        # Test 5: Mixed existing and non-existing playlist names
        mixed_playlists = ["homepageNatif", "nonExistentModule", "algolia"]
        result_mixed = contentful_client.set_contentful_modules(mixed_playlists)

        # Verify: Should return only existing modules
        assert (
            len(result_mixed) == 2
        ), "Should return only existing modules from mixed list"
        mixed_names = [module["name"] for module in result_mixed]
        assert (
            "homepageNatif" in mixed_names
        ), "Should include existing homepageNatif module"
        assert "algolia" in mixed_names, "Should include existing algolia module"
        assert (
            "nonExistentModule" not in mixed_names
        ), "Should not include non-existent module"

        # Test 6: Verify filtering doesn't modify original data
        result_all_again = contentful_client.set_contentful_modules([])
        assert len(result_all_again) == len(
            result_empty
        ), "Original data should not be modified by filtering"

    def test_get_playlists_orchestrates_full_workflow(
        self, contentful_client, mock_contentful_client
    ):
        """Test that get_playlists properly orchestrates the complete ETL workflow."""
        # Setup: Mock HTTP get call for tags (needed by get_basic_fields)
        mock_tag_response = MagicMock()
        mock_tag_response.json.return_value = {"name": "test-tag-name"}
        mock_contentful_client._http_get.return_value = mock_tag_response

        # Setup: Create mock entries for different content types
        mock_homepage_entry = MagicMock()
        mock_homepage_entry.id = "homepage-123"
        mock_homepage_entry.title = "Test Homepage"

        # Mock sys attributes for homepage entry
        mock_space = MagicMock()
        mock_space.id = "test-space-id"
        mock_content_type = MagicMock()
        mock_content_type.id = "homepageNatif"
        mock_environment = MagicMock()
        mock_environment.id = "test"

        mock_homepage_entry.sys = {
            "id": "homepage-123",
            "content_type": mock_content_type,
            "created_at": datetime(2023, 1, 1),
            "updated_at": datetime(2023, 1, 2),
            "space": mock_space,
            "environment": mock_environment,
            "revision": 1,
            "type": "Entry",
            "locale": "en-US",
        }

        # Mock submodule for homepage
        mock_submodule = MagicMock()
        mock_submodule.id = "submodule-456"

        # Mock fields for homepage entry
        mock_homepage_entry.fields.return_value = {
            "title": "Test Homepage",
            "modules": [mock_submodule],  # Homepage has submodules
        }

        # Mock metadata with tags
        mock_tag = MagicMock()
        mock_tag.id = "test-tag-123"
        mock_homepage_entry._metadata = {"tags": [mock_tag]}

        # Setup: Mock entries response for get_paged_modules
        mock_entries_response = MagicMock()
        mock_entries_response.total = 1
        mock_entries_response.__iter__ = lambda x: iter([mock_homepage_entry])
        mock_entries_response.__len__ = lambda x: 1
        mock_contentful_client.entries.return_value = mock_entries_response

        # Setup: Configure contentful_modules to only test homepageNatif (simpler scenario)
        contentful_client.contentful_modules = [
            {
                "name": "homepageNatif",
                "additional_fields": ["title", "modules"],
                "children": [],  # No children for simpler test
            }
        ]

        # Execute: Run the main orchestration method
        df_modules, df_links, df_tags = contentful_client.get_playlists()

        # Verify: Return types should be DataFrames
        assert isinstance(df_modules, pd.DataFrame), "Should return modules DataFrame"
        assert isinstance(df_links, pd.DataFrame), "Should return links DataFrame"
        assert isinstance(df_tags, pd.DataFrame), "Should return tags DataFrame"

        # Verify: Modules DataFrame should have data
        assert len(df_modules) > 0, "Modules DataFrame should have at least one entry"

        # Verify: Module data should be processed correctly
        first_module = df_modules.iloc[0]
        assert (
            first_module["id"] == "homepage-123"
        ), "Module ID should be correctly processed"
        assert (
            first_module["title"] == "Test Homepage"
        ), "Module title should be correctly processed"
        assert (
            first_module["content_type"] == "homepageNatif"
        ), "Content type should be correctly processed"

        # Verify: Links DataFrame should have parent-child relationships
        assert (
            len(df_links) > 0
        ), "Links DataFrame should have parent-child relationships for homepage"
        first_link = df_links.iloc[0]
        assert (
            first_link["parent"] == "homepage-123"
        ), "Parent ID should be correctly set"
        assert (
            first_link["child"] == "submodule-456"
        ), "Child ID should be correctly set"

        # Verify: Tags DataFrame should have tag data
        assert len(df_tags) > 0, "Tags DataFrame should have tag data"
        first_tag = df_tags.iloc[0]
        assert (
            first_tag["tag_id"] == "test-tag-123"
        ), "Tag ID should be correctly processed"
        assert (
            first_tag["tag_name"] == "test-tag-name"
        ), "Tag name should be correctly processed"
        assert (
            first_tag["entry_id"] == "homepage-123"
        ), "Entry ID should be correctly associated with tag"

        # Verify: DataFrame columns should be properly structured
        expected_module_columns = {
            "id",
            "title",
            "content_type",
            "space",
            "environment",
            "created_at",
            "updated_at",
            "revision",
            "type",
            "locale",
            "date_imported",
            "contentful_tags_id",
            "contentful_tags_name",
        }
        actual_module_columns = set(df_modules.columns)
        assert expected_module_columns.issubset(
            actual_module_columns
        ), "Modules DataFrame should have expected columns"

        assert list(df_links.columns) == [
            "parent",
            "child",
        ], "Links DataFrame should have parent and child columns"
        assert list(df_tags.columns) == [
            "tag_id",
            "tag_name",
            "entry_id",
        ], "Tags DataFrame should have tag columns"

        # Verify: None values should be replaced with NaN
        assert (
            not df_modules.isin(["None"]).any().any()
        ), "None strings should be replaced with NaN"

        # Verify: The method should return the same DataFrames as the instance attributes
        assert (
            df_modules is contentful_client.df_modules
        ), "Should return the instance's modules DataFrame"
        assert (
            df_links is contentful_client.df_links
        ), "Should return the instance's links DataFrame"
        assert (
            df_tags is contentful_client.df_tags
        ), "Should return the instance's tags DataFrame"
