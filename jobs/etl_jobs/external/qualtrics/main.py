import pandas as pd
import typer
import time
from utils import (
    DATA_CENTER,
    DIRECTORY_ID,
    API_TOKEN,
    OPT_OUT_EXPORT_COLUMNS,
    save_to_raw_bq,
    save_partition_table_to_bq,
    IR_PRO_TABLE_SCHEMA,
    IR_JEUNES_TABLE_SCHEMA,
    ANSWERS_SCHEMA,
)
from qualtrics_opt_out import import_qualtrics_opt_out
from qualtrics_survey_answers import (
    QualtricsSurvey,
    import_survey_metadata,
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
    )
):
    if task == "import_opt_out_users":
        import_qualtrics_opt_out(
            DATA_CENTER, DIRECTORY_ID, API_TOKEN, OPT_OUT_EXPORT_COLUMNS
        )
    elif task == "import_ir_survey_answers":
        dfs = []
        for target, survey_id in ir_surveys_mapping.items():
            if target == "pro":
                qualtrics_survey = QualtricsSurvey(
                    api_token=API_TOKEN, survey_id=survey_id, data_center=DATA_CENTER
                )
                qualtrics_survey.get_qualtrics_survey()
                processed_df = qualtrics_survey.process_ir_qualtrics_data(target)
                save_to_raw_bq(
                    processed_df,
                    f"qualtrics_answers_ir_survey_{target}",
                    IR_PRO_TABLE_SCHEMA,
                )
            else:
                qualtrics_survey = QualtricsSurvey(
                    api_token=API_TOKEN, survey_id=survey_id, data_center=DATA_CENTER
                )
                qualtrics_survey.get_qualtrics_survey()
                processed_df = qualtrics_survey.process_ir_qualtrics_data(target)
                dfs.append(processed_df)
        jeunes_df = pd.concat(dfs)
        save_to_raw_bq(
            jeunes_df, f"qualtrics_answers_ir_survey_jeunes", IR_JEUNES_TABLE_SCHEMA
        )

    elif task == "import_all_survey_answers":
        active_surveys = import_survey_metadata(
            data_center=DATA_CENTER, api_token=API_TOKEN
        )
        dfs = []
        i = 0
        for s_id in active_surveys:
            i = i + 1
            survey = QualtricsSurvey(s_id, DATA_CENTER, API_TOKEN)
            survey.get_qualtrics_survey()
            df = survey.process_survey_answers()
            df["survey_int_id"] = df["survey_id"].apply(
                lambda survey_id: abs(hash(str(survey_id)) % 1000000007)
            )
            save_partition_table_to_bq(
                df, f"qualtrics_answers", ANSWERS_SCHEMA, "survey_int_id"
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
