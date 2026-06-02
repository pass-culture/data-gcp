import pandas as pd
from loguru import logger

from schemas import FORMAT_DICT


def process_survey_answers(df: pd.DataFrame, survey_id: str) -> pd.DataFrame:
    columns = df.columns
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
    logger.info(
        f"[{survey_id}] answer_cols={len(answer_columns)} other_cols={len(other_columns)}"
    )
    mapping_question_str = df[answer_columns].iloc[0].to_dict()
    mapping_question_id = df[answer_columns].iloc[1].to_dict()

    data = df.iloc[2:].reset_index(drop=True)

    extra_data = data[other_columns].to_dict(orient="records") if other_columns else None

    core_id_vars = [c for c in system_columns if c in columns]
    df_final = pd.melt(
        data[core_id_vars + answer_columns],
        id_vars=core_id_vars,
        value_vars=answer_columns,
        var_name="question",
        value_name="answer",
    ).assign(
        question_str=lambda _df: _df["question"].map(mapping_question_str),
        question_id=lambda _df: _df["question"].map(mapping_question_id),
    )


    if extra_data is not None:
        df_final["extra_data"] = extra_data * len(answer_columns)

    df_final = df_final.dropna(subset=["answer"])
    logger.info(f"[{survey_id}] melt done: {len(df_final)} rows")

    df_final["survey_id"] = survey_id
    df_final["survey_int_id"] = abs(hash(str(survey_id)) % 1000000007)

    rename_dict = {
        "StartDate": "start_date",
        "EndDate": "end_date",
        "Status": "status",
        "RecordedDate": "recorded_date",
        "ResponseId": "response_id",
        "ExternalReference": "user_id",
        "DistributionChannel": "distribution_channel",
    }
    df_final = df_final.rename(columns=rename_dict).astype(FORMAT_DICT)

    return df_final[
        (~df_final["question_id"].str.contains("TEXT", na=False))
        | (df_final["question_id"].str.contains("Topics", na=False))
    ]
