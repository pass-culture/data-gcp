import pandas as pd
import requests
from utils import (
    DATA_CENTER,
    DIRECTORY_ID,
    API_TOKEN,
    OPT_OUT_EXPORT_COLUMNS,
    save_to_raw_bq,
)
from qualtrics_opt_out import import_qualtrics_opt_out
from qualtrics_survey_answers import QualtricsSurvey

ir_surveys_mapping = {
    "GRANT_15_17": "SV_3IdnHqrnsuS17oy",
    "GRANT_18": "SV_cBV3xaZ92BoW5sW",
    "pro": "SV_eOOPuFjgZo1emR8",
}


def run(request):

    request_json = request.get_json(silent=True)
    request_args = request.args

    if request_json and "task" in request_json:
        task = request_json["task"]
    elif request_args and "task" in request_args:
        task = request_args["task"]
    else:
        raise RuntimeError("You need to provide a task argument.")

    if task == "import_opt_out_users":
        import_qualtrics_opt_out(
            DATA_CENTER, DIRECTORY_ID, API_TOKEN, OPT_OUT_EXPORT_COLUMNS
        )
    elif task == "import_ir_survey_answers":
        dfs = []
        for target, survey_id in ir_surveys_mapping.items():
            qualtrics_survey = QualtricsSurvey(
                api_token=API_TOKEN, survey_id=survey_id, data_center=DATA_CENTER
            )
            dfs.append(qualtrics_survey.process_qualtrics_data(target))
        processed_df = pd.concat(dfs)
        save_to_raw_bq(processed_df, "qualtrics_answers_ir_survey")
    else:
        raise RuntimeError(
            "Task argument must be one of import_opt_out_users or import_ir_survey_answers."
        )

    return "Success"
