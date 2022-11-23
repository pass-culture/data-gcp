import pandas as pd
import re
from utils import ANALYTICS_DATASET, ENVIRONMENT_SHORT_NAME


def get_data_archiving(sql_file):
    """Run SQL query and save data in a dataframe."""

    params = {"{{ANALYTICS_DATASET}}": ANALYTICS_DATASET}

    file = open(sql_file, "r")
    sql = file.read()

    for param, table_name in params.items():
        sql = sql.replace(param, table_name)

    archives_df = pd.read_gbq(sql, dialect="standard")

    return archives_df


def preprocess_data_archiving(
    _df,
    object_type,
    parent_folder_to_archive,
    limit_inactivity_in_days,
):

    # Compute the number of days since last activity of a card
    _df = _df.assign(
        days_since_last_execution=lambda _df: pd.to_datetime("today")
        - _df["last_execution_date"].dt.tz_localize(None)
    )

    if ENVIRONMENT_SHORT_NAME == "prod":
        _df["destination_collection_id"] = (  # Get the archive collection id
            _df["archive_location_level_2"]
            .dropna()
            .str.split("/")
            .apply(lambda x: x[-1])
        )
        _df = _df[
            ~_df["archive_location_level_2"].isna()
        ]  # Remove elements for which destination collection is empty.
        # Filter the inactive cards in the folder we want to archive.
        _df = _df[
            (
                _df["days_since_last_execution"]
                >= pd.to_timedelta(limit_inactivity_in_days, unit="d")
            )
            & (_df["parent_folder"].isin(parent_folder_to_archive))
        ]

    if ENVIRONMENT_SHORT_NAME == "dev" or ENVIRONMENT_SHORT_NAME == "stg":
        _df["destination_collection_id"] = 29
        # In dev, we consider a card to be inactive after 1 day of inactivity.
        _df = _df[(_df["days_since_last_execution"] >= pd.to_timedelta(1, unit="d"))]

    if object_type == "card":
        _df_to_archive = (
            _df.drop("collection_id", axis=1)
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


class move_to_archive:
    def __init__(self, movement, metabase, gcp_project, analytics_dataset):
        self.movement = movement
        self.id = movement.get("id", None)
        self.name = movement.get("name", None)
        self.object_type = movement.get("object_type", None)
        self.destination_collection = movement.get("destination_collection_id", None)
        self.collection_id = movement.get("collection_id", None)
        self.last_execution_date = movement.get("last_execution_date", None)
        self.last_execution_context = movement.get("last_execution_context", None)
        self.parent_folder = movement.get("parent_folder", None)
        self.gcp_project = gcp_project
        self.analytics_dataset = analytics_dataset

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
            f"""{self.gcp_project}.{self.analytics_dataset}.logs_metabase_archiving""",
            project_id=self.gcp_project,
            if_exists="append",
        )
        return "Logs saved in BQ"
