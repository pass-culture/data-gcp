import gspread
import pandas as pd
from google.oauth2 import service_account


SPREADSHEET_ID = "1eht2OUf9eOdGXYbGa10zhJ0sDe5SENuTS8WKYJbjikc"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

EXPECTED_HEADERS_DICT = {
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
}


def export_sheet(sa_info):
    credentials = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SCOPES
    )
    gc = gspread.authorize(credentials)
    sh = gc.open_by_key(SPREADSHEET_ID)
    worksheet = sh.get_worksheet(0)
    raw_columns = list(EXPECTED_HEADERS_DICT.keys())

    df = pd.DataFrame(worksheet.get_all_records(expected_headers=raw_columns))[
        raw_columns
    ]
    df = df.rename(columns=EXPECTED_HEADERS_DICT)
    for _c in df.columns:
        df[_c] = df[_c].astype(str)
    return df
