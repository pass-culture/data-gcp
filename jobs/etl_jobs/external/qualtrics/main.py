import time

import typer

from qualtrics_opt_out import import_qualtrics_opt_out
from qualtrics_survey_answers import (
    QualtricsSurvey,
    import_survey_metadata,
)
from utils import (
    ANSWERS_SCHEMA,
    API_TOKEN,
    DATA_CENTER,
    DIRECTORY_ID,
    OPT_OUT_EXPORT_COLUMNS,
    save_partition_table_to_bq,
)

ir_surveys_mapping = {
    "GRANT_15_17": "SV_3IdnHqrnsuS17oy",
    "GRANT_18": "SV_cBV3xaZ92BoW5sW",
    "pro": "SV_eOOPuFjgZo1emR8",
}


def run(
    task: str = typer.Option(
        ...,
        help="Nom de la tache",
    ),
):
    if task == "import_opt_out_users":
        import_qualtrics_opt_out(
            DATA_CENTER, DIRECTORY_ID, API_TOKEN, OPT_OUT_EXPORT_COLUMNS
        )

    elif task == "import_all_survey_answers":
        active_surveys = import_survey_metadata(
            data_center=DATA_CENTER, api_token=API_TOKEN
        )
        i = 0
        for s_id in active_surveys:
            i = i + 1
            survey = QualtricsSurvey(
                api_token=API_TOKEN, survey_id=s_id, data_center=DATA_CENTER
            )
            survey.get_qualtrics_survey()
            df = survey.process_survey_answers()
            df["survey_int_id"] = df["survey_id"].apply(
                lambda survey_id: abs(hash(str(survey_id)) % 1000000007)
            )
            save_partition_table_to_bq(
                df, "qualtrics_answers", ANSWERS_SCHEMA, "survey_int_id"
            )
            if i % 10:
                time.sleep(60)

    else:
        raise RuntimeError(
            "Task argument must be one of import_opt_out_users or import_ir_survey_answers."
        )

    return "Success"


if __name__ == "__main__":
    typer.run(run)
