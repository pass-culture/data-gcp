import requests
import zipfile
import io
import pandas as pd


class QualtricsSurvey:
    def __init__(self, api_token: str, survey_id: str, data_center: str):
        self.api_token = api_token
        self.survey_id = survey_id
        self.data_center = data_center

    def get_survey_answers(self) -> pd.DataFrame:
        # Setting user Parameters
        file_format = "csv"

        # Setting static parameters
        request_check_progress = 0
        progress_status = "in progress"
        base_url = "https://{0}.qualtrics.com/API/v3/responseexports/".format(
            self.data_center
        )
        headers = {
            "content-type": "application/json",
            "x-api-token": self.api_token,
        }

        # Step 1: Creating Data Export
        download_request_url = base_url
        download_request_payload = (
            '{"format":"' + file_format + '","surveyId":"' + self.survey_id + '"}'
        )  # you can set useLabels:True to get responses in text format
        download_request_response = requests.request(
            "POST", download_request_url, data=download_request_payload, headers=headers
        )
        progress_id = download_request_response.json()["result"]["id"]

        # Step 2: Checking on Data Export Progress and waiting until export is ready
        while request_check_progress < 100 and progress_status != "complete":
            request_check_url = base_url + progress_id
            request_check_response = requests.request(
                "GET", request_check_url, headers=headers
            )
            request_check_progress = request_check_response.json()["result"][
                "percentComplete"
            ]

        # Step 3: Downloading file
        request_download_url = base_url + progress_id + "/file"
        request_download = requests.request(
            "GET", request_download_url, headers=headers, stream=True
        )
        print(f"Downloaded answers from qualtrics survey {self.survey_id}")

        # Step 4: Unzipping the file
        file = zipfile.ZipFile(io.BytesIO(request_download.content))
        name = file.namelist()[0]
        df = pd.read_csv(file.open(name))

        print(f"Loaded answers from {name} in a dataframe")

        return df

    def process_qualtrics_data(self, target: str) -> pd.DataFrame:

        response_df = self.get_survey_answers()

        if target == "jeunes_18":
            questions_columns = ["Q1", "Q3"]
        elif target == "jeunes_15_17":
            questions_columns = ["Q1", "Q2"]
        else:
            questions_columns = ["Q1", "Q2", "Q3"]

        response_df = response_df[
            ["ResponseID", "StartDate", "EndDate", "ExternalDataReference", "Finished"]
            + questions_columns
        ]

        mapping_question = response_df[questions_columns][:2].to_dict(orient="records")
        mapping_question_str = mapping_question[0]
        mapping_question_id = mapping_question[1]
        for key, value in mapping_question_id.items():
            mapping_question_id[key] = eval(value)["ImportId"]
        columns_mapping = {
            "ResponseID": "answer_id",
            "StartDate": "start_date",
            "EndDate": "end_date",
            "ExternalDataReference": "user_id",
            "Finished": "finished",
            "level_5": "question",
            0: "answer",
        }

        response_processed = (
            response_df[2:]
            .set_index(
                [
                    "ResponseID",
                    "StartDate",
                    "EndDate",
                    "ExternalDataReference",
                    "Finished",
                ]
            )
            .stack()
            .reset_index()
            .rename(columns=columns_mapping)
            .assign(
                question_str=lambda _df: _df["question"].map(mapping_question_str),
                question_id=lambda _df: _df["question"].map(mapping_question_id),
                user_type=target,
            )
        )

        return response_processed
