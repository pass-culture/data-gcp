import re

import pandas as pd

from utils import (
    ENVIRONMENT_SHORT_NAME,
    INT_METABASE_DATASET,
    PROJECT_NAME,
)


class ListArchive:
    def __init__(self, metabase_folder, rules):
        self.metabase_folder = metabase_folder
        self.rules = rules[0]
        self.rules_sql = self.rules.get("rule_archiving_sql")

    def get_data_archiving(self):
        """Run SQL query and save data in a dataframe."""
        print("📢 get_data_archiving() is called")

        if self.rules_sql:
            query = (
                f"""SELECT * FROM `{INT_METABASE_DATASET}.activity` {self.rules_sql}"""
            )
            self.archive_df = pd.read_gbq(query)
            print(
                "✅ ",
                len(self.archive_df),
                " cards are ready to be archived in folder ",
                self.metabase_folder,
            )
        else:
            print("⚠️ self.rules_sql is emplty or None")

    def preprocess_data_archiving(self, object_type):
        print("📢 preprocess_data_archiving() is called")
        try:
            self.archive_df[
                "destination_collection_id"
            ] = (  # Get the archive collection id
                self.archive_df["archive_location_level_2"]
                .dropna()
                .str.split("/")
                .apply(lambda x: x[-1])
            )
            self.archive_df = self.archive_df[
                ~self.archive_df["archive_location_level_2"].isna()
            ]  # Remove elements for which destination collection is empty.
        except Exception:
            print(
                "⚠️ Error retrieving destination_collection_id in ",
                ENVIRONMENT_SHORT_NAME,
            )
            self.archive_df["destination_collection_id"] = 29

        if object_type == "card":
            _df_to_archive = (
                self.archive_df.drop("collection_id", axis=1)
                .rename(
                    columns={
                        "card_id": "id",
                        "card_name": "name",
                        "card_collection_id": "collection_id",
                    }
                )
                .assign(
                    object_type=object_type
                )  # Take the min of the 'archive_question' boolean to be more restrictive
            )

            _dict_to_archive = (  # Stock data in a dict
                _df_to_archive[
                    [
                        "id",
                        "name",
                        "object_type",
                        "collection_id",
                        "destination_collection_id",
                        "total_users",
                        "total_views",
                        "nbr_dashboards",
                        "last_execution_date",
                        "last_execution_context",
                        "total_errors",
                        "parent_folder",
                        "days_since_last_execution",
                    ]
                ]
                .assign(
                    destination_collection_id=lambda _df: _df[
                        "destination_collection_id"
                    ].astype(int)
                )
                .sort_values("id")
                .drop_duplicates()
                .to_dict(orient="records")
            )

            return _dict_to_archive


class MoveToArchive:
    def __init__(self, movement, metabase):
        self.movement = movement
        self.id = movement.get("id", None)
        self.name = movement.get("name", None)
        self.object_type = movement.get("object_type", None)
        self.destination_collection = movement.get("destination_collection_id", None)
        self.collection_id = movement.get("collection_id", None)
        self.last_execution_date = movement.get("last_execution_date", None)
        self.last_execution_context = movement.get("last_execution_context", None)
        self.parent_folder = movement.get("parent_folder", None)
        self.metabase_connection = metabase

    def rename_archive_object(self):
        """Rename the object to archive. Add ['Archive - '] prefix to object name."""

        if re.search("archive", self.name, re.IGNORECASE):
            archive_name = self.name
        else:
            archive_name = "[Archive] - " + self.name

        params = {"name": archive_name}

        if self.object_type == "card":
            result = self.metabase_connection.put_card(self.id, params)

        return result

    def move_object(self):
        """Move the object from its original collection to the archive collection."""

        id_list = [self.id]

        if self.object_type == "card":
            result = self.metabase_connection.update_card_collections(
                id_list, self.destination_collection
            )

        archived_logs = []

        if result.get("status") == "ok":
            archived_logs.append(
                {
                    "id": self.id,
                    "object_type": self.object_type,
                    "status": "success",
                    "new_collection_id": self.destination_collection,
                    "previous_collection_id": self.collection_id,
                    "archived_at": pd.Timestamp.now(),
                    "last_execution_date": self.last_execution_date,
                    "last_execution_context": self.last_execution_context,
                    "parent_folder": self.parent_folder,
                }
            )
        else:
            archived_logs.append(
                {
                    "id": self.id,
                    "object_type": self.object_type,
                    "status": result.get("status"),
                    "new_collection_id": self.destination_collection,
                    "previous_collection_id": self.collection_id,
                    "archived_at": pd.Timestamp.now(),
                    "last_execution_date": self.last_execution_date,
                    "last_execution_context": self.last_execution_context,
                    "parent_folder": self.parent_folder,
                }
            )
        return archived_logs

    def save_logs_bq(self):
        """Save the movement in a BQ table."""

        archived_logs_dict = self.move_object()
        archived_logs_df = pd.DataFrame(archived_logs_dict)
        archived_logs_df.to_gbq(
            f"""{PROJECT_NAME}.{INT_METABASE_DATASET}.archiving_log""",
            project_id=PROJECT_NAME,
            if_exists="append",
        )
        return "Logs saved in BQ"
