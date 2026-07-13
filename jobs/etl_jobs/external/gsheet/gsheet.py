import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account
from gspread.http_client import BackOffHTTPClient
from loguru import logger

from gcp import SA_EXTRA_SCOPES


def validate_headers(actual_headers, spreadsheet_id, expected_headers):
    missing = set(expected_headers) - set(actual_headers)
    unexpected = set(actual_headers) - set(expected_headers)
    if missing:
        raise ValueError(
            f"Spreadsheet {spreadsheet_id} header mismatch — "
            f"missing: {sorted(missing)}, "
            f"unexpected: {sorted(unexpected)}, "
            f"found: {actual_headers}"
        )
    logger.info(f"Headers validated for spreadsheet: {spreadsheet_id}")


def fetch_worksheet(gc, spreadsheet_id, expected_headers):
    sh = gc.open_by_key(spreadsheet_id)
    worksheet = sh.get_worksheet(0)
    rows = worksheet.get_all_values()
    actual_headers = rows[0] if rows else []
    validate_headers(actual_headers, spreadsheet_id, expected_headers)
    df = pd.DataFrame(rows[1:], columns=actual_headers)[expected_headers]
    df = df.replace("", np.nan)
    df = df.dropna(how="all", axis=0)
    return df


def export_sheet(sa_info, sheet_details):
    credentials = service_account.Credentials.from_service_account_info(
        sa_info, scopes=SA_EXTRA_SCOPES
    )
    gc = gspread.authorize(credentials, http_client=BackOffHTTPClient)
    expected_headers = list(sheet_details["expected_headers_dict"].keys())
    dfs = []
    for spreadsheet_id in sheet_details["spreadsheet_ids"]:
        logger.info(f"Fetching spreadsheet: {spreadsheet_id}")
        df = fetch_worksheet(gc, spreadsheet_id, expected_headers)
        df = df.rename(columns=sheet_details["expected_headers_dict"])
        for col in df.columns:
            df[col] = df[col].astype(str)
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True)
