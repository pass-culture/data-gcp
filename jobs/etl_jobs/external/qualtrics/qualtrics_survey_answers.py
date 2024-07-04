import requests
import zipfile
import io
import pandas as pd
from utils import ENV_SHORT_NAME


def import_survey_metadata(data_center, api_token):
    base_url = f"https://{data_center}.qualtrics.com/API/v3/surveys/"

    headers = {
        "Accept": "application/json",
        "x-api-token": api_token,
    }

    response = requests.get(base_url, headers=headers)
    if response.json()["meta"]["httpStatus"] == "200 - OK":
        surveys = pd.DataFrame(response.json()["result"]["elements"])
        surveys.to_gbq(f"clean_{ENV_SHORT_NAME}.qualtrics_survey", if_exists="replace")
        active_surveys = list(surveys[surveys.isActive == True].id)
        return active_surveys


class QualtricsSurvey:
    def __init__(self, api_token: str, survey_id: str, data_center: str):
        self.api_token = api_token
        self.survey_id = survey_id
        self.data_center = data_center

    def get_qualtrics_survey(self) -> pd.DataFrame:
        # Setting user Parameters
        file_format = "csv"

        # Setting static parameters
        request_check_progress = 0
        progress_status = "in progress"
        base_url = f"https://{self.data_center}.qualtrics.com/API/v3/surveys/{self.survey_id}/export-responses/"
        headers = {
            "content-type": "application/json",
            "x-api-token": self.api_token,
        }

        # Step 1: Creating Data Export
        download_request_url = base_url
        download_request_payload = (
            '{"format":"' + file_format + '"}'
        )  # you can set useLabels:True to get responses in text format
        download_request_response = requests.request(
            "POST", download_request_url, data=download_request_payload, headers=headers
        )
        progress_id = download_request_response.json()["result"]["progressId"]

        # Step 2: Checking on Data Export Progress and waiting until export is ready
        while request_check_progress < 100 and progress_status != "complete":
            request_check_url = base_url + progress_id
            request_check_response = requests.request(
                "GET", request_check_url, headers=headers
            )
            request_check_progress = request_check_response.json()["result"][
                "percentComplete"
            ]

        file_id = request_check_response.json()["result"]["fileId"]

        answers_request_url = f"https://{self.data_center}.qualtrics.com/API/v3/surveys/{self.survey_id}/export-responses/{file_id}/file"

        answers_request_response = requests.request(
            "GET", answers_request_url, headers=headers
        )

        file = zipfile.ZipFile(io.BytesIO(answers_request_response.content))
        name = file.namelist()[0]
        df = pd.read_csv(file.open(name))
        ### Export to raw here ?
        print("Downloaded qualtrics survey")
        self.raw_answer_df = df

    def process_ir_qualtrics_data(self, target: str) -> pd.DataFrame:
        response_df = self.raw_answer_df
        nb_columns = response_df.shape[1]

        if target == "GRANT_18":
            questions_columns = ["Q1", "Q3"]
            select_fields = [
                "StartDate",
                "EndDate",
                "ResponseId",
                "ExternalReference",
                "user_type",
                "question",
                "question_id",
                "question_str",
                "answer",
                "Q3_Topics",
                "theoretical_amount_spent",
                "user_activity",
                "user_civility",
            ]
        elif target == "GRANT_15_17":
            questions_columns = ["Q1", "Q2"]
            select_fields = [
                "StartDate",
                "EndDate",
                "ResponseId",
                "ExternalReference",
                "user_type",
                "question",
                "question_id",
                "question_str",
                "answer",
                "Q2_Topics",
                "theoretical_amount_spent",
                "user_activity",
                "user_civility",
            ]
        else:
            questions_columns = ["Q1", "Q2"]
            select_fields = [
                "StartDate",
                "EndDate",
                "ResponseId",
                "ExternalReference",
                "user_type",
                "question",
                "question_id",
                "question_str",
                "answer",
                "Q1_Topics",
                "anciennete_jours",
                "non_cancelled_bookings",
                "offers_created",
            ]

        mapping_question = response_df[questions_columns][:2].to_dict(orient="records")
        mapping_question_str = mapping_question[0]
        mapping_question_id = mapping_question[1]
        for key, value in mapping_question_id.items():
            mapping_question_id[key] = eval(value)["ImportId"]

        columns_mapping = {
            f"level_{nb_columns - len(questions_columns)}": "question",
            0: "answer",
        }
        columns_to_drop = ["RecipientFirstName", "RecipientLastName", "RecipientEmail"]

        response_processed = (
            response_df[2:]
            .set_index(list(response_df.columns.drop(questions_columns)))
            .stack()
            .reset_index()
            .rename(columns=columns_mapping)
            .drop(columns=columns_to_drop)
            .assign(
                question_str=lambda _df: _df["question"].map(mapping_question_str),
                question_id=lambda _df: _df["question"].map(mapping_question_id),
                user_type=target,
            )
        )

        response_processed.columns = (
            response_processed.columns.str.normalize("NFKD")
            .str.encode("ascii", errors="ignore")
            .str.decode("utf-8")  # remove accents
            .str.replace("[.,(,),-]", "")
            .str.replace("-", "")
            .str.replace("  ", " ")
            .str.replace(" ", "_")
        )

        response_processed = response_processed.astype(str)

        return response_processed[select_fields]

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
        mapping_question = self.raw_answer_df[answer_columns][:2].to_dict(
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
            .stack(dropna=False)
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

        format_dict = {
            "start_date": str,
            "end_date": str,
            "status": int,
            "response_id": str,
            "user_id": str,
            "distribution_channel": str,
            "question": str,
            "answer": str,
            "question_str": str,
            "question_id": str,
            "extra_data": str,
            "survey_id": str,
            "survey_int_id": int,
        }

        df_final = df_final.rename(columns=rename_dict).astype(format_dict)

        df_final["survey_id"] = self.survey_id

        filtered_df = df_final[
            (~df_final["question_id"].str.contains("TEXT"))
            | (df_final["question_id"].str.contains("Topics"))
        ]

        return filtered_df
