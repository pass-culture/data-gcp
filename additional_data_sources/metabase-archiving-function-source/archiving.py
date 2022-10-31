import os
import pandas as pd
import requests
from datetime import datetime
import time

import pandas_gbq as pd_gbq


def get_data_archiving(sql_file):
    """Run SQL query and save data in a dataframe."""

    file = open(sql_file, "r")
    sql = file.read()
    archives_df = pd.read_gbq(sql, dialect="standard")

    return archives_df


def get_dashboard_archiving_rules(row):
    """Get rules to archive or not a dashboard."""

    if (
        row["question_context"] == 1 and row["dashboard_context"] == 1
    ):  # if inactivity comes from inactivity in card AND inactivity in dashboard
        return True  # => ARCHIVE dashboard
    elif (
        row["question_context"] == 1 and row["dashboard_context"] == 0
    ):  # if inactivity comes from inactivity in card only
        return False  # => DO NOT ARCHIVE dashboard
    elif (
        row["question_context"] == 0 and row["dashboard_context"] == 0
    ):  # if inactivity does not comes from an inactivity in card neither an inactivity in dashboard
        return False  # => DO NOT ARCHIVE dashboard
    elif (
        row["question_context"] == 0 and row["dashboard_context"] == 1
    ):  # if inactivity comes from inactivity in dashboard only
        return True  # => ARCHIVE dashboard
    else:
        return False


def get_question_archiving_rules(row):
    """Get rules to archive or not a question."""

    if (
        row["question_context"] == 1 and row["dashboard_context"] == 1
    ):  # if inactivity comes from inactivity in card AND inactivity in dashboard
        return True  # => ARCHIVE question
    elif (
        row["question_context"] == 1 and row["dashboard_context"] == 0
    ):  # if inactivity comes from inactivity in card only
        if row["nb_dashboards"] <= 1:
            return True
        else:
            return False  # => DO NOT ARCHIVE question
    elif (
        row["question_context"] == 0 and row["dashboard_context"] == 0
    ):  # if inactivity does not comes from an inactivity in card neither an inactivity in dashboard
        return False  # => DO NOT ARCHIVE question
    elif (
        row["question_context"] == 0 and row["dashboard_context"] == 1
    ):  # if inactivity comes from inactivity in dashboard only
        return False  # => DO NOT ARCHIVE question
    else:
        return False


def preprocess_data_archiving(_df, object_type):
    """Preprocess the data to archive."""
    print(f"{_df.shape[0]} - Start: card that might be removed...")
    dashboard_count = (
        _df.sort_values("card_id")
        .groupby("card_id")["dashboard_id"]
        .count()
        .reset_index()
        .rename(columns={"dashboard_id": "nb_dashboards"})
    )

    _df = _df.merge(dashboard_count, how="left", on="card_id")
    print(f"{_df.shape[0]} - Add the number of dashboards...")

    _df = pd.concat(
        [
            _df[_df["nb_dashboards"] != 0].query(
                "dashboard_id.notnull()"
            ),  # remove rows where card are associated to dashboard but dashboard_id is null
            _df[_df["nb_dashboards"] == 0],
        ]
    )
    print(f"{_df.shape[0]} - Remove dashboard_id null...")

    _df = _df[
        ~_df["archive_location_level_2"].isna()
    ]  # Remove elements for which destination collection is empty.
    print(
        f"{_df.shape[0]} - Remove elements for which destination collection is empty..."
    )
    _df["destination_collection_id"] = (  # Get the archive collection id
        _df["archive_location_level_2"].dropna().str.split("/").apply(lambda x: x[-1])
    )

    _df_deduplicated = _df[
        [  # Drop duplicates
            "card_id",
            "dashboard_id",
            "dashboard_name",
            "context",
            "card_name",
            "destination_collection_id",
            "nb_dashboards",
        ]
    ].drop_duplicates()
    print(f"{_df_deduplicated.shape[0]} - Drop duplicates...")

    _df_deduplicated["question_context"] = (
        _df_deduplicated["context"] == "question"
    )  # Flag if context of inactivity is question
    _df_deduplicated["dashboard_context"] = (
        _df_deduplicated["context"] == "dashboard"
    )  # Flag if context of inactivity is dashboard

    _df_agregated = (  # Agregate context of incativity by card/dashboard pair
        _df_deduplicated.groupby(
            [
                "card_id",
                "dashboard_id",
                "card_name",
                "dashboard_name",
                "destination_collection_id",
                "nb_dashboards",
            ],
            dropna=False,
        )
        .sum()
        .reset_index()
    )
    print(f"{_df_agregated.shape[0]} - Aggregated...")
    _df_agregated["archive_dashboard"] = _df_agregated.apply(
        get_dashboard_archiving_rules, axis=1
    )  # Get archiving rule for dashboard (depending on context)
    _df_agregated["archive_question"] = _df_agregated.apply(
        get_question_archiving_rules, axis=1
    )  # Get archiving rule for question (depending on context)

    _df_agregated.drop(["question_context", "dashboard_context"], axis=1, inplace=True)

    if object_type == "card":
        _df_to_archive = (
            _df_agregated.rename(columns={"card_id": "id", "card_name": "name"})
            .assign(object_type=object_type)
            .groupby(["id", "name", "object_type", "destination_collection_id"])[
                "archive_question"
            ]
            .min()  # Take the min of the 'archive_question' boolean to be more restrictive
        )

        _df_to_archive = _df_to_archive[
            _df_to_archive == True
        ].reset_index()  # Keep cards with archiving_flag = true

        _dict_to_archive = (  # Stock data in a dict
            _df_to_archive[["id", "name", "object_type", "destination_collection_id"]]
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

    # if object_type == "dashboard":


class move_to_archive:
    def __init__(self, movement, metabase, gcp_project, analytics_dataset):
        self.movement = movement
        self.id = movement.get("id", None)
        self.name = movement.get("name", None)
        self.object_type = movement.get("object_type", None)
        self.destination_collection = movement.get("destination_collection_id", None)
        self.gcp_project = gcp_project
        self.analytics_dataset = analytics_dataset

        self.metabase_connection = metabase

    def rename_archive_object(self):
        """Rename the object to archive. Add ['Archive - '] prefix to object name."""

        name_lower = self.name.lower()

        if "archive" in name_lower:
            name_lower = name_lower.replace("archive", "").strip(" -[]")
        name_archive = "[Archive] - " + name_lower

        params = {"name": name_archive}

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
                    "archived_at": pd.Timestamp.now(),
                }
            )
        else:
            archived_logs.append(
                {
                    "id": self.id,
                    "object_type": self.object_type,
                    "status": result.get("status"),
                    "new_collection_id": self.destination_collection,
                    "archived_at": pd.Timestamp.now(),
                }
            )
        return archived_logs

    def save_logs_bq(self):
        """Save the movement in a BQ table."""

        archived_logs_dict = self.move_object()
        archived_logs_df = pd.DataFrame(archived_logs_dict)
        archived_logs_df.to_gbq(
            f"""{self.gcp_project}.{self.analytics_dataset}.logs_metabase_archiving""",
            project_id=self.gcp_project,
            if_exists="append",
        )
        return "Logs saved in BQ"
