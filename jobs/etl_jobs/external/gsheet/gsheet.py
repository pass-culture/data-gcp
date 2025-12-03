import gspread
import numpy as np
import pandas as pd
from google.oauth2 import service_account

SHEETS = {
    "gsheet_backoffice_log_cross_reference": {
        "spreadsheet_ids": ["1IvXNbkMM6sDaQtViB03ng3P-_vl05v1yW6-Yp4med3g"],
        "expected_headers_dict": {
            "log_extract": "log_extract",
            "log_type": "log_type",
            "log_name": "log_name",
        },
    },
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
            "19cYAL0apbcPmoLwEIOmR3eqHZePxMyChBtzqytTXr7o",  # MA Final (sept-dec 2025-2026) -
            "12YxzAQdpAdlZS6HAOZqYsA6bPfS1CsbuWcbpfpIX7CI",  # MASA Final (sept-dec 2025-2026) -
            "1EFiRSptEeheoaHyjJogjjClNmzNHgrxG9colYlg1uIY",  # Aix-Marseille Provisional (2025-2026) -
            "1Gzljf7ZHEgj_5G8A-S6HUQ7H7cZ1qK2T12xBxJ5s8oQ",  # MEN Final (sept-dec 2025-2026) -
            "1FuEZ7Y1Yk2Tje1_3l-9sQ8W8pCyEz2BkDlXteKuXvw0",  # SEM Final (sept-dev 2025-2026) -
            "1ntswOFC_eG_OCisuTJN8cwKp8dPOms1-MamQQbcLXTg",  # MASA Final (2024-2025) -
            "1fbrPv2mgo09xAtZ4BiQcM5B6ufwunCn6ymtCjJJQaaY",  # MEN Final (2024-2025) -
            "1H8ZH4IWVhtvCS6T0smSMrRu490rrNZIl--7dpRbersc",  # MASA Provisional (2024-2025)
            "1oKulYPFMNaJaZ3gpK-kuYnBKJxngQFwjQ4rA1YTWMDM",  # SEM Provisional (2024-2025)
            "1WeG4CG5DRUdmvZe7sS47onGBk5nRND4VQzk2qAoNK1U",  # Aix-Marseille Provisional (2024-2025)
            "1AwclRi49IJaTO3CfjegYu9siYFVobA839vNhYjsEgeM",  # MENJ Provisional (2024-2025)
            "1DVneUFuBhQTQS6I8tN1pcOuelsglwOyBnKVsQn4MlE8",  # MA Provisional (2024-2025)
            "1J5l4zsGJiKLmYV68EjnsiN_8X7BFkR48fIecFgzhdkU",  # Historical DATA
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
            "region": "region",
            "event": "event",
        },
    },
    "gsheet_instagram_campaign_tag": {
        "spreadsheet_ids": ["1pHcY7jtHB4CkU2caib3i4fLhUCmacqqMkh6U8YLjqAI"],
        "expected_headers_dict": {
            "date": "date",
            "post_name": "post_name",
            "media_url": "media_url",
            "media_id": "media_id",
            "macro_objective": "macro_objective",
            "micro_objective": "micro_objective",
            "offer_category": "offer_category",
            "region": "region",
            "event": "event",
        },
    },
    "gsheet_institution_meg_scholar_group": {
        "spreadsheet_ids": ["1zeh1Jbi_002q7OjioL2ErStXbEUv2j2ooOyLPjMH2gk"],
        "expected_headers_dict": {
            "institution_id": "institution_id",
            "institution_external_id": "institution_external_id",
            "institution_name": "institution_name",
            "meg_id": "meg_id",
            "school_group_name": "school_group_name",
        },
    },
    "gsheet_movie_arthouse_and_heritage_label": {
        "spreadsheet_ids": ["1zNoM1F0B09jNfPSGIioLxFU7911R0DREP9sL94CN2BY"],
        "expected_headers_dict": {
            "movie_status": "movie_status",
            "movie_visa": "movie_visa",
            "movie_title": "movie_title",
            "movie_director": "movie_director",
            "movie_distributor": "movie_distributor",
            "movie_release_date": "movie_release_date",
            "Label Recherche & Découverte": "movie_is_research_discovery_label",
            "Soutien Inédits": "movie_is_unreleased_support",
            "15/25": "movie_is_15_25",
            "Soutien GNCR": "movie_is_gncr_support",
            "Soutien ACID": "movie_is_acid_support",
            "Label Patrimoine & Répertoire": "movie_is_heritage_repertory_label",
            "Label Jeune Public": "movie_is_young_audience_label",
            "movie_production_country": "movie_production_country",
        },
    },
    "gsheet_masa_affiliation_details_2324": {
        "spreadsheet_ids": ["1zRRO50DprjnBiWsb83vZESGmCoA8gT52qm1eCFtfkrs"],
        "expected_headers_dict": {
            "UAI": "institution_external_id",
            "Ministère": "ministry",
            "Année scolaire": "scholar_year",
            "Provisoire": "is_provisional",
            "Secteur": "sector",
            "Affiliation": "affiliation",
        },
    },
    "gsheet_company_contact": {
        "spreadsheet_ids": ["1sc-khJYKwaVCf20lV5QBoqciUjGpjBSaKJpWbG5FHJg"],
        "expected_headers_dict": {
            "Nom": "last_name",
            "Prénom": "first_name",
            "Direction": "direction",
            "Département": "team",
            "Email": "email",
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
