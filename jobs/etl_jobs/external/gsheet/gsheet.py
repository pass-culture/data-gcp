import gspread
import pandas as pd
from google.oauth2 import service_account

SHEETS = {
    "gsheet_eac_webinar": {
        "spreadsheet_id": "1eht2OUf9eOdGXYbGa10zhJ0sDe5SENuTS8WKYJbjikc",
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
    "sensitive_item_recommendation": {
        "spreadsheet_id": "1wdZvDmM25BOckRwdaaVoCpDWNsQns1W5ZQ7i5B5BQGE",
        "expected_headers_dict": {
            "date": "date",
            "item_id": "item_id",
            "reason": "reason",
        },
    },
}

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def export_sheet(sa_info, sheet_details):
    credentials = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES
    )
    gc = gspread.authorize(credentials)
    sh = gc.open_by_key(sheet_details["spreadsheet_id"])
    worksheet = sh.get_worksheet(0)
    raw_columns = list(sheet_details["expected_headers_dict"].keys())

    df = pd.DataFrame(worksheet.get_all_records(expected_headers=raw_columns))[
        raw_columns
    ]
    df = df.rename(columns=sheet_details["expected_headers_dict"])
    for _c in df.columns:
        df[_c] = df[_c].astype(str)
    return df
