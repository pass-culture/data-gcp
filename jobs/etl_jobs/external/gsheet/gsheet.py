import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account

SHEETS = {
    "gsheet_eac_webinar": {
        "spreadsheet_ids": ["1eht2OUf9eOdGXYbGa10zhJ0sDe5SENuTS8WKYJbjikc"],
        "expected_headers_dict": {
            "Date": "date",
            "First Name": "first_name",
            "Last Name": "last_name",
            "Email": "email",
            "Registration Time": "registration_time",
            "Approval Status": "approval_status",
            "Quel est le nom de votre structure ?": "offerer_name",
            "Job Title": "job_title",
            "Quel est le SIREN de votre structure ?": "siren",
            "Quelle est votre région": "region_name",
            "Dans quel domaine culturel agissez vous principalement ?": "cultural_domain",
        },
    },
    "gsheet_ml_recommendation_sensitive_item": {
        "spreadsheet_ids": ["1wdZvDmM25BOckRwdaaVoCpDWNsQns1W5ZQ7i5B5BQGE"],
        "expected_headers_dict": {
            "date": "date",
            "item_id": "item_id",
            "reason": "reason",
        },
    },
    "gsheet_educational_institution_student_headcount": {
        "spreadsheet_ids": [
            "1DLvov_JtnwvHuejKHnFoLKG6ULYKDBqyslMqs_sPCMc",  # MASA
            "1Oy9FGcpyK_GvjphDYUD7un4xRDtobDWNI9Ha9iNU_L0",  # SEM
            "1BvieY_RTjOzQMDB2rMe4QqwSPqoH7a5ARTMmSGYgNnk",  # Aix-Marseille
            "1gUd_QIEHPMD03XZA1bHG19Dgk1v8u0ioU4SGHPLIshk",  # MENJ
            "15_JNWINeU7nfSbU6subTMK0utr_av1Z3d5-UKD2CBn8",  # Historical DATA
        ],
        "expected_headers_dict": {
            "Année scolaire": "school_year",
            "Ministère": "ministry",
            "UAI": "uai",
            "Provisoire": "is_provisional",
            "Classe": "class",
            "Montant par élève": "amount_per_student",
            "Effectif": "headcount",
        },
    },
    "gsheet_tiktok_campaign_tag": {
        "spreadsheet_ids": ["197DKa9c5TuwvOsUzhLc3Gb5ld1IhfzUsNYYaBvlfBho"],
        "expected_headers_dict": {
            "date": "date",
            "post_name": "post_name",
            "tiktotk_id": "tiktotk_id",
            "macro_objective": "macro_objective",
            "micro_objective": "micro_objective",
            "offer_category": "offer_category",
        },
    },
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def export_sheet(sa_info, sheet_details):
    credentials = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES
    )
    dfs = []
    for spreadsheet_id in sheet_details["spreadsheet_ids"]:
        gc = gspread.authorize(credentials)
        sh = gc.open_by_key(spreadsheet_id)
        worksheet = sh.get_worksheet(0)
        raw_columns = list(sheet_details["expected_headers_dict"].keys())

        df = pd.DataFrame(worksheet.get_all_records(expected_headers=raw_columns))[
            raw_columns
        ]
        df = df.replace("", np.nan)
        df = df.dropna(how="all", axis=0)
        df = df.rename(columns=sheet_details["expected_headers_dict"])
        for _c in df.columns:
            df[_c] = df[_c].astype(str)

        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)
