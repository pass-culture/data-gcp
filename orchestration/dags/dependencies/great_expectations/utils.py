import pandas as pd
import os
from datetime import datetime, timedelta

DAG_FOLDER = os.environ.get("DAG_FOLDER", "dags/")

ge_root_dir = f"{DAG_FOLDER}/great_expectations/"

today = datetime.now().strftime("%Y-%m-%d")
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
last_week = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")


def get_table_volume_bounds(partition_field, dataset_name, table_name, nb_days):
    """This function returns the daily confiance interval
    for a table volume (with level of 10%).
    """

    query = f"""
    WITH daily_volume as (
        SELECT { partition_field }, count(*) as volume
        FROM `{ dataset_name }.{ table_name }` 
        WHERE { partition_field } between current_date() - {nb_days} - 1 and current_date() - 1
        GROUP BY 1
    )
    select avg(volume) as avg_volume
    from daily_volume
    """

    df = pd.read_gbq(query)

    return [df["avg_volume"][0] * 0.9, df["avg_volume"][0] * 1.1]


def clear_directory(path, directory_name):
    full_path = path + directory_name
    if os.path.exists(full_path):
        for file in os.listdir(full_path):
            os.remove(os.path.join(full_path, file))
    else:
        os.makedirs(full_path)
    return os.listdir(path)
