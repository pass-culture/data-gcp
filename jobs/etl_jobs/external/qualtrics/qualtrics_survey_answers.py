import io
import zipfile

import pandas as pd
import pandas_gbq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from schemas import FORMAT_DICT
from utils import BIGQUERY_RAW_DATASET, PROJECT_NAME

_RETRY = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])


def import_survey_metadata(data_center, api_token):
    base_url = f"https://{data_center}.qualtrics.com/API/v3/surveys/"

    headers = {
        "Accept": "application/json",
        "x-api-token": api_token,
    }

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=_RETRY))
    response = session.get(base_url, headers=headers)
    if response.json()["meta"]["httpStatus"] == "200 - OK":
        surveys = pd.DataFrame(response.json()["result"]["elements"])
        pandas_gbq.to_gbq(
            surveys,
            f"{BIGQUERY_RAW_DATASET}.qualtrics_survey",
            project_id=PROJECT_NAME,
            if_exists="replace",
        )
        active_surveys = surveys.loc[lambda df: df.isActive].id.tolist()
        return active_surveys


class QualtricsSurvey:
    def __init__(self, api_token: str, survey_id: str, data_center: str):
        self.api_token = api_token
        self.survey_id = survey_id
        self.data_center = data_center
        self.session = requests.Session()
        self.session.headers.update({"content-type": "application/json", "x-api-token": api_token})
        self.session.mount("https://", HTTPAdapter(max_retries=_RETRY))

    def get_qualtrics_survey(self) -> pd.DataFrame:
        # Setting user Parameters
        file_format = "csv"

        # Setting static parameters
        request_check_progress = 0
        progress_status = "in progress"
        base_url = f"https://{self.data_center}.qualtrics.com/API/v3/surveys/{self.survey_id}/export-responses/"

        # Step 1: Creating Data Export
        download_request_url = base_url
        download_request_payload = (
            '{"format":"' + file_format + '"}'
        )  # you can set useLabels:True to get responses in text format
        download_request_response = self.session.request(
            "POST", download_request_url, data=download_request_payload
        )
        progress_id = download_request_response.json()["result"]["progressId"]

        # Step 2: Checking on Data Export Progress and waiting until export is ready
        while request_check_progress < 100 and progress_status != "complete":
            request_check_url = base_url + progress_id
            request_check_response = self.session.request(
                "GET", request_check_url
            )
            request_check_progress = request_check_response.json()["result"][
                "percentComplete"
            ]

        file_id = request_check_response.json()["result"]["fileId"]

        answers_request_url = f"https://{self.data_center}.qualtrics.com/API/v3/surveys/{self.survey_id}/export-responses/{file_id}/file"

        answers_request_response = self.session.request(
            "GET", answers_request_url
        )

        file = zipfile.ZipFile(io.BytesIO(answers_request_response.content))
        name = file.namelist()[0]
        df = pd.read_csv(file.open(name))
        ### Export to raw here ?
        print("Downloaded qualtrics survey")
        self.raw_answer_df = df

    def process_survey_answers(self) -> pd.DataFrame:
        columns = self.raw_answer_df.columns
        system_columns = [
            "StartDate",
            "EndDate",
            "Status",
            "ResponseId",
            "ExternalReference",
            "DistributionChannel",
        ]
        answer_columns = [col for col in columns if col.startswith("Q")]
        drop_columns = ["RecipientLastName", "RecipientFirstName", "RecipientEmail"]
        other_columns = [
            col
            for col in columns
            if col not in system_columns + answer_columns + drop_columns
        ]
        mapping_question = self.raw_answer_df[answer_columns].iloc[:2].to_dict(
            orient="records"
        )
        mapping_question_str = mapping_question[0]
        mapping_question_id = mapping_question[1]

        nb_columns = self.raw_answer_df.shape[1]
        columns_mapping = {
            f"level_{nb_columns - len(answer_columns)}": "question",
            0: "answer",
        }

        df_step1 = (
            self.raw_answer_df[2:]
            .set_index(list(self.raw_answer_df.columns.drop(answer_columns)))
            .stack()
            .reset_index()
            .rename(columns=columns_mapping)
            .assign(
                question_str=lambda _df: _df["question"].map(mapping_question_str),
                question_id=lambda _df: _df["question"].map(mapping_question_id),
            )
        )
        if len(other_columns) > 0:
            df_final = df_step1.assign(
                extra_data=lambda _df: _df[other_columns].to_dict(orient="records")
            ).drop(drop_columns + other_columns, axis=1)
        else:
            df_final = df_step1.drop(drop_columns + other_columns, axis=1)

        rename_dict = {
            "StartDate": "start_date",
            "EndDate": "end_date",
            "Status": "status",
            "RecordedDate": "recorded_date",
            "ResponseId": "response_id",
            "ExternalReference": "user_id",
            "DistributionChannel": "distribution_channel",
        }

        df_final["survey_id"] = self.survey_id

        df_final = df_final.rename(columns=rename_dict).astype(FORMAT_DICT)

        # Prevent crashes on unmapped or missing values with na=False in contains method 
        filtered_df = df_final[
            (~df_final["question_id"].str.contains("TEXT", na=False))
            | (df_final["question_id"].str.contains("Topics", na=False))
        ]

        return filtered_df
