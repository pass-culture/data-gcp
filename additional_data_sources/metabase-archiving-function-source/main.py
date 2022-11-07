import os
import pandas as pd
import requests
from datetime import datetime
import time

import pandas_gbq as pd_gbq
from google.cloud import secretmanager

from metabase_api import MetabaseAPI
from archiving import get_data_archiving, preprocess_data_archiving, move_to_archive
from utils import (
    PROJECT_NAME,
    ENVIRONMENT_SHORT_NAME,
    METABASE_API_USERNAME,
    METABASE_HOST,
    ANALYTICS_DATASET,
    access_secret_data,
    max_cards_to_archive,
    sql_file,
    parent_folder_to_archive,
    limit_inactivity_in_days,
)

if ENVIRONMENT_SHORT_NAME == "dev" or ENVIRONMENT_SHORT_NAME == "stg":
    version_id = 2
else:
    version_id = 1
password = access_secret_data(
    PROJECT_NAME, f"metabase-api-secret-{ENVIRONMENT_SHORT_NAME}", version_id=version_id
)
metabase = MetabaseAPI(
    username=METABASE_API_USERNAME, password=password, host=METABASE_HOST
)


def run(request):
    archives_df = get_data_archiving(sql_file)

    archives_dicts = preprocess_data_archiving(
        archives_df,
        object_type="card",
        parent_folder_to_archive=parent_folder_to_archive,
        limit_inactivity_in_days=limit_inactivity_in_days,
    )

    for card in archives_dicts[:max_cards_to_archive]:
        archiving = move_to_archive(
            movement=card,
            metabase=metabase,
            gcp_project=PROJECT_NAME,
            analytics_dataset=ANALYTICS_DATASET,
        )
        archiving.move_object()
        archiving.rename_archive_object()
        archiving.save_logs_bq()
        time.sleep(1)

    return "success"
