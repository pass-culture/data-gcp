from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd
import requests
import time
from scripts.utils import GCP_PROJECT, BIGQUERY_CLEAN_DATASET, TOKEN

LIMIT_DATE_SIREN = 35
MAX_SIREN_CALL = 150


def get_limit_date():
    limit_date = (datetime.now() - timedelta(days=LIME_DATE_SIREN)).strftime("%Y-%m-%d")
    return limit_date


def get_offerer_siren_list():
    siren_list = []
    client = bigquery.Client()
    query_job = client.query(
        f"""SELECT offerer_siren as siren
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offerer` 
        WHERE offerer_siren is not null
        """
    )
    rows = query_job.result()
    for row in rows:
        siren_list.append(row.siren)
    return siren_list


def get_siren_query(siren_list):
    query = "https://api.insee.fr/entreprises/sirene/V3/siren?q="
    for siren in siren_list:
        query += f"""siren:{siren} OR """
    query += f"""siren:{siren_list[len(siren_list)-1]}&curseur=*&nombre=1000"""
    return query


def append_info_siren_list(siren_info_list, result):
    for unitesLegales in result["unitesLegales"]:
        try:
            siren_info_list.append(
                {
                    "siren": unitesLegales["siren"],
                    "unite_legale": unitesLegales["categorieEntreprise"],
                    "derniertraitement": unitesLegales[
                        "dateDernierTraitementUniteLegale"
                    ],
                    "dateCreationUniteLegale": unitesLegales["dateCreationUniteLegale"],
                    "identifiantAssociationUniteLegale": unitesLegales[
                        "identifiantAssociationUniteLegale"
                    ],
                    "trancheEffectifsUniteLegale": unitesLegales[
                        "trancheEffectifsUniteLegale"
                    ],
                    "anneeEffectifsUniteLegale": unitesLegales[
                        "anneeEffectifsUniteLegale"
                    ],
                    "dateDernierTraitementUniteLegale": unitesLegales[
                        "dateDernierTraitementUniteLegale"
                    ],
                    "categorieEntreprise": unitesLegales["categorieEntreprise"],
                    "etatAdministratifUniteLegale": unitesLegales[
                        "periodesUniteLegale"
                    ][0]["etatAdministratifUniteLegale"],
                    "nomUniteLegale": unitesLegales["periodesUniteLegale"][0][
                        "nomUniteLegale"
                    ],
                    "denominationUniteLegale": unitesLegales["periodesUniteLegale"][0][
                        "denominationUniteLegale"
                    ],
                    "categorieJuridiqueUniteLegale": unitesLegales[
                        "periodesUniteLegale"
                    ][0]["categorieJuridiqueUniteLegale"],
                    "activitePrincipaleUniteLegale": unitesLegales[
                        "periodesUniteLegale"
                    ][0]["activitePrincipaleUniteLegale"],
                    "changementCategorieJuridiqueUniteLegale": unitesLegales[
                        "periodesUniteLegale"
                    ][0]["changementCategorieJuridiqueUniteLegale"],
                    "nomenclatureActivitePrincipaleUniteLegale": unitesLegales[
                        "periodesUniteLegale"
                    ][0]["nomenclatureActivitePrincipaleUniteLegale"],
                    "economieSocialeSolidaireUniteLegale": unitesLegales[
                        "periodesUniteLegale"
                    ][0]["economieSocialeSolidaireUniteLegale"],
                    "caractereEmployeurUniteLegale": unitesLegales[
                        "periodesUniteLegale"
                    ][0]["caractereEmployeurUniteLegale"],
                }
            )
        except:
            siren_info_list.append(
                {
                    "siren": unitesLegales["siren"],
                    "unite_legale": None,
                    "derniertraitement": None,
                    "dateCreationUniteLegale": None,
                    "identifiantAssociationUniteLegale": None,
                    "trancheEffectifsUniteLegale": None,
                    "anneeEffectifsUniteLegale": None,
                    "dateDernierTraitementUniteLegale": None,
                    "categorieEntreprise": None,
                    "etatAdministratifUniteLegale": None,
                    "nomUniteLegale": None,
                    "denominationUniteLegale": None,
                    "categorieJuridiqueUniteLegale": None,
                    "activitePrincipaleUniteLegale": None,
                    "changementCategorieJuridiqueUniteLegale": None,
                    "nomenclatureActivitePrincipaleUniteLegale": None,
                    "economieSocialeSolidaireUniteLegale": None,
                    "caractereEmployeurUniteLegale": None,
                }
            )

            pass

    return siren_info_list


# put token secrets
def query_siren():
    siren_info_list = []
    headers = {
        "Accept": "application/json",
        "Authorization": f"""Bearer {TOKEN}""",
    }
    siren_list = get_offerer_siren_list()
    nb_df_sub_divisions = len(siren_list) // MAX_SIREN_CALL
    if (len(siren_list) - nb_df_sub_divisions * MAX_SIREN_CALL) == 0:
        nb_df_sub_divisions -= 1
    for k in range(nb_df_sub_divisions + 1):
        query = get_siren_query(
            siren_list[k * MAX_SIREN_CALL : (k + 1) * MAX_SIREN_CALL]
        )
        response = requests.get(
            query,
            headers=headers,
        )
        if response.status_code != 200:
            raise ValueError(
                f"Error API CALL {response.status_code} : {response.reason}"
            )
        else:
            result = response.json()
            siren_info_list = append_info_siren_list(siren_info_list, result)
        time.sleep(2)
    return siren_info_list


def siren_to_bq():
    save_to_bq(query_siren())
    return


def save_to_bq(siren_list):
    pd.DataFrame(siren_list).to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.siren_data""",
        project_id=GCP_PROJECT,
        if_exists="replace",
    )
    return
