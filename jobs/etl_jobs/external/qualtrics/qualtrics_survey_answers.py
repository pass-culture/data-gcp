import pandas as pd

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
    mapping_question = df[answer_columns].iloc[:2].to_dict(orient="records")
    mapping_question_str = mapping_question[0]
    mapping_question_id = mapping_question[1]

    nb_columns = df.shape[1]
    columns_mapping = {
        f"level_{nb_columns - len(answer_columns)}": "question",
        0: "answer",
    }

    df_step1 = (
        df[2:]
        .set_index(list(df.columns.drop(answer_columns)))
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
