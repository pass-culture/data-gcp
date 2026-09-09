import time
from datetime import datetime, timedelta

import pandas as pd
import pandas_gbq
import requests
from google.cloud import bigquery

from scripts.utils import (
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_INT_RAW_DATASET,
    GCP_PROJECT,
    access_secret_data,
)

MAX_SIRET_CALL = 100
MAX_SIRET_TO_UPDATE = 5000


def get_venue_siret_list():
    last_seven_days = datetime.now() - timedelta(days=7)
    siret_list = []
    client = bigquery.Client()

    table_exists_query = f"""
        SELECT COUNT(*) AS cnt
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.INFORMATION_SCHEMA.TABLES`
        WHERE table_name = 'siret_data'
    """
    table_exists = next(client.query(table_exists_query).result()).cnt > 0

    if table_exists:
        query = f"""
            WITH updated_recently AS (
                SELECT DISTINCT siret
                FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.siret_data`
                WHERE date(update_date) >= date('{last_seven_days.strftime("%Y-%m-%d")}')
            )
            SELECT DISTINCT v.venue_siret AS siret
            FROM `{GCP_PROJECT}.{BIGQUERY_INT_RAW_DATASET}.venue` v
            LEFT JOIN updated_recently ur ON ur.siret = v.venue_siret
            WHERE v.venue_siret IS NOT NULL
              AND LENGTH(v.venue_siret) = 14
              AND ur.siret IS NULL
            ORDER BY RAND()
        """
    else:
        query = f"""
            SELECT DISTINCT v.venue_siret AS siret
            FROM `{GCP_PROJECT}.{BIGQUERY_INT_RAW_DATASET}.venue` v
            WHERE v.venue_siret IS NOT NULL
              AND LENGTH(v.venue_siret) = 14
            ORDER BY RAND()
        """

    for row in client.query(query).result():
        siret_list.append(row.siret)

    if len(siret_list) > MAX_SIRET_TO_UPDATE:
        siret_list = siret_list[:MAX_SIRET_TO_UPDATE]

    return siret_list


def get_siret_query(siret_list):
    terms = [f"siret:{siret}" for siret in siret_list]
    return (
        "https://api.insee.fr/api-sirene/3.11/siret?q="
        + " OR ".join(terms)
        + "&nombre=1000"
    )


def append_info_siret_list(siret_info_list, result):
    for etablissement in result.get("etablissements", []):
        period = (etablissement.get("periodesEtablissement") or [{}])[0]
        unite_legale = etablissement.get("uniteLegale") or {}

        siret_info_list.append(
            {
                "siret": etablissement.get("siret"),
                "siren": etablissement.get("siren"),
                "nic": etablissement.get("nic"),
                "dateCreationEtablissement": etablissement.get(
                    "dateCreationEtablissement"
                ),
                "trancheEffectifsEtablissement": etablissement.get(
                    "trancheEffectifsEtablissement"
                ),
                "anneeEffectifsEtablissement": etablissement.get(
                    "anneeEffectifsEtablissement"
                ),
                "dateDernierTraitementEtablissement": etablissement.get(
                    "dateDernierTraitementEtablissement"
                ),
                "etablissementSiege": etablissement.get("etablissementSiege"),
                "etatAdministratifEtablissement": period.get(
                    "etatAdministratifEtablissement"
                ),
                "enseigne1Etablissement": period.get("enseigne1Etablissement"),
                "denominationUsuelleEtablissement": period.get(
                    "denominationUsuelleEtablissement"
                ),
                "activitePrincipaleEtablissement": period.get(
                    "activitePrincipaleEtablissement"
                ),
                "nomenclatureActivitePrincipaleEtablissement": period.get(
                    "nomenclatureActivitePrincipaleEtablissement"
                ),
                "categorieEntreprise": unite_legale.get("categorieEntreprise"),
                "etatAdministratifUniteLegale": unite_legale.get(
                    "etatAdministratifUniteLegale"
                ),
                "denominationUniteLegale": unite_legale.get("denominationUniteLegale"),
                "nomUniteLegale": unite_legale.get("nomUniteLegale"),
                "categorieJuridiqueUniteLegale": unite_legale.get(
                    "categorieJuridiqueUniteLegale"
                ),
            }
        )

    return siret_info_list


def query_siret():
    api_key = access_secret_data(GCP_PROJECT, "siren-key")
    siret_info_list = []
    headers = {
        "Accept": "application/json",
        "X-INSEE-Api-Key-Integration": api_key,
    }

    siret_list = get_venue_siret_list()
    print(f"Will update {len(siret_list)} SIRET")

    if len(siret_list) == 0:
        return siret_info_list

    nb_df_sub_divisions = len(siret_list) // MAX_SIRET_CALL
    if (len(siret_list) - nb_df_sub_divisions * MAX_SIRET_CALL) == 0:
        nb_df_sub_divisions -= 1

    for k in range(nb_df_sub_divisions + 1):
        batch = siret_list[k * MAX_SIRET_CALL : (k + 1) * MAX_SIRET_CALL]
        query = get_siret_query(batch)
        response = requests.get(query, headers=headers, timeout=60)

        if response.status_code == 200:
            result = response.json()
            siret_info_list = append_info_siret_list(siret_info_list, result)
        elif response.status_code == 404:
            print("Error 404")
            print(response.json())
        else:
            raise ValueError(
                f"Error API CALL {response.status_code} : {response.reason}"
            )

        time.sleep(2.5)

    if len(siret_info_list) == 0:
        print("Something went wrong for all SIRET.... pass.")

    return siret_info_list


def siret_to_bq():
    results = query_siret()
    if len(results) > 0:
        save_to_bq(results)


def save_to_bq(siret_list):
    df = pd.DataFrame(siret_list)
    df["update_date"] = datetime.now().strftime("%Y-%m-%d")
    pandas_gbq.to_gbq(
        df,
        f"{BIGQUERY_CLEAN_DATASET}.siret_data",
        project_id=GCP_PROJECT,
        if_exists="append",
    )
