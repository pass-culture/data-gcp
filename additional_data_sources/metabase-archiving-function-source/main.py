import os
import pandas as pd
import requests
from datetime import datetime
import time

import pandas_gbq as pd_gbq
from google.cloud import secretmanager

from metabase_api import MetabaseAPI
from archiving import (
    get_data_archiving,
    preprocess_data_archiving,
    move_to_archive
)
from utils import (
    PROJECT_NAME,
    access_secret_data,
    ENVIRONMENT_SHORT_NAME,
    METABASE_API_USERNAME,
    METABASE_HOST,
    ANALYTICS_DATASET
)

sql_file = 'sql/archiving_query.sql'
password = access_secret_data(PROJECT_NAME, f"metabase-api-secret-{ENVIRONMENT_SHORT_NAME}")
metabase = MetabaseAPI(username=METABASE_API_USERNAME, password=password, host=METABASE_HOST)

def run():
    archives_df = get_data_archiving(sql_file)
    
    archives_dicts = preprocess_data_archiving(archives_df, object_type='card')
    
    for card in archives_dicts[:10]:
        move_to_archive = move_to_archive(movement=card, metabase=metabase, gcp_project=PROJECT_NAME, analytics_dataset=ANALYTICS_DATASET)
        move_to_archive.move_object()
        move_to_archive.rename_archive_object()
        move_to_archive.save_logs_bq()
        time.sleep(1)