from datetime import date
from google.cloud import storage
import json
from loguru import logger
import time
from pcreco.utils.env_vars import (
    DATA_BUCKET,
    QPI_FOLDER,
    log_duration,
)


def get_cold_start_categories_from_gcs(user_id):
    start = time.time()
    logger.info(f"get_cold_start_categories from gcs : {user_id}")
    todays_date = date.today().strftime("%Y%m%d")
    filepath = f"{QPI_FOLDER}/qpi_answers_{todays_date}/user_id_{user_id}.jsonl"
    cold_start_categories = []
    try:
        qpi_raw = _get_qpi_file_from_gcs(filepath)
        if qpi_raw:
            user_answer_ids = []
            for answers in qpi_raw["answers"]:
                for answers_id in answers["answer_ids"]:
                    user_answer_ids.append(answers_id)
            cold_start_categories = sorted(user_answer_ids)
        logger.info(
            f"get_cold_start_categories from gcs: file found with {cold_start_categories}"
        )
        log_duration("get_cold_start_categories from gcs", start)
    except:
        logger.info(f"get_cold_start_categories: not found ")
        cold_start_categories = ["UKN"]
    log_duration(f"get_cold_start_categories from gcs : {user_id} ", start)
    return cold_start_categories


def _get_qpi_file_from_gcs(filepath):
    client = storage.Client()
    bucket = client.get_bucket(DATA_BUCKET)
    blob = bucket.get_blob(filepath)
    with blob.open("r") as f:
        qpi_raw = json.load(f)
    return qpi_raw
