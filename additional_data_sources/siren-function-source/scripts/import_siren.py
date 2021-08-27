from datetime import datetime, timedelta
import pandas as pd
import requests
import time
from scripts.utils import GCP_PROJECT, BIGQUERY_CLEAN_DATASET, BUCKET_NAME, TOKEN

SIREN_FILENAME = "siren_data.csv"
LIME_DATE_SIREN = 35
MAX_SIREN_CALL = 150


def get_limit_date():
    limit_date = (datetime.now() - timedelta(days=LIME_DATE_SIREN)).strftime("%Y-%m-%d")
    return limit_date


def get_offerer_siren_list():
    return pd.read_gbq(
        f"""SELECT offerer_siren as siren
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offerer` 
        WHERE offerer_siren is not null
        """
    )


def get_siren_query(siren_list):
    query = "https://api.insee.fr/entreprises/sirene/V3/siren?q="
    for i in range(len(siren_list) - 1):
        query += "".join(f"""siren:{siren_list[i]} OR """)
    query += f"""siren:{siren_list[len(siren_list)-1]}&curseur=*&nombre=1000"""
    return query


def append_info_siren_list(siren_info_list, result):
    for i in range(len(result["unitesLegales"])):
        try:
            siren_info_list.append(
                {
                    "siren": result["unitesLegales"][i]["siren"],
                    "unite_legale": result["unitesLegales"][i]["categorieEntreprise"],
                    "derniertraitement": result["unitesLegales"][i][
                        "dateDernierTraitementUniteLegale"
                    ],
                    "dateCreationUniteLegale": result["unitesLegales"][i][
                        "dateCreationUniteLegale"
                    ],
                    "identifiantAssociationUniteLegale": result["unitesLegales"][i][
                        "identifiantAssociationUniteLegale"
                    ],
                    "trancheEffectifsUniteLegale": result["unitesLegales"][i][
                        "trancheEffectifsUniteLegale"
                    ],
                    "anneeEffectifsUniteLegale": result["unitesLegales"][i][
                        "anneeEffectifsUniteLegale"
                    ],
                    "dateDernierTraitementUniteLegale": result["unitesLegales"][i][
                        "dateDernierTraitementUniteLegale"
                    ],
                    "categorieEntreprise": result["unitesLegales"][i][
                        "categorieEntreprise"
                    ],
                    "etatAdministratifUniteLegale": result["unitesLegales"][i][
                        "periodesUniteLegale"
                    ][0]["etatAdministratifUniteLegale"],
                    "nomUniteLegale": result["unitesLegales"][i]["periodesUniteLegale"][
                        0
                    ]["nomUniteLegale"],
                    "denominationUniteLegale": result["unitesLegales"][i][
                        "periodesUniteLegale"
                    ][0]["denominationUniteLegale"],
                    "categorieJuridiqueUniteLegale": result["unitesLegales"][i][
                        "periodesUniteLegale"
                    ][0]["categorieJuridiqueUniteLegale"],
                    "activitePrincipaleUniteLegale": result["unitesLegales"][i][
                        "periodesUniteLegale"
                    ][0]["activitePrincipaleUniteLegale"],
                    "changementCategorieJuridiqueUniteLegale": result["unitesLegales"][
                        i
                    ]["periodesUniteLegale"][0][
                        "changementCategorieJuridiqueUniteLegale"
                    ],
                    "nomenclatureActivitePrincipaleUniteLegale": result[
                        "unitesLegales"
                    ][i]["periodesUniteLegale"][0][
                        "nomenclatureActivitePrincipaleUniteLegale"
                    ],
                    "economieSocialeSolidaireUniteLegale": result["unitesLegales"][i][
                        "periodesUniteLegale"
                    ][0]["economieSocialeSolidaireUniteLegale"],
                    "caractereEmployeurUniteLegale": result["unitesLegales"][i][
                        "periodesUniteLegale"
                    ][0]["caractereEmployeurUniteLegale"],
                }
            )
        except:
            siren_info_list.append(
                {
                    "siren": result["unitesLegales"][i]["siren"],
                    "unite_legale": "Not found",
                    "derniertraitement": "Not found",
                    "dateCreationUniteLegale": "Not found",
                    "identifiantAssociationUniteLegale": "Not found",
                    "trancheEffectifsUniteLegale": "Not found",
                    "anneeEffectifsUniteLegale": "Not found",
                    "dateDernierTraitementUniteLegale": "Not found",
                    "categorieEntreprise": "Not found",
                    "etatAdministratifUniteLegale": "Not found",
                    "nomUniteLegale": "Not found",
                    "denominationUniteLegale": "Not found",
                    "categorieJuridiqueUniteLegale": "Not found",
                    "activitePrincipaleUniteLegale": "Not found",
                    "changementCategorieJuridiqueUniteLegale": "Not found",
                    "nomenclatureActivitePrincipaleUniteLegale": "Not found",
                    "economieSocialeSolidaireUniteLegale": "Not found",
                    "caractereEmployeurUniteLegale": "Not found",
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
    if siren_list.shape[0] > MAX_SIREN_CALL:
        nb_df_sub_divisions = siren_list.shape[0] // MAX_SIREN_CALL
        for k in range(nb_df_sub_divisions):
            temp_siren_list = siren_list[k * MAX_SIREN_CALL : (k + 1) * MAX_SIREN_CALL]
            temp_siren_list.reset_index(drop=True, inplace=True)
            query = get_siren_query(temp_siren_list["siren"])
            response = requests.get(
                query,
                headers=headers,
            )
            if response.status_code != 200:
                print("error")
            result = response.json()
            siren_info_list = append_info_siren_list(siren_info_list, result)
            time.sleep(2)
        temp_siren_list = siren_list[nb_df_sub_divisions * MAX_SIREN_CALL :]
        temp_siren_list.reset_index(drop=True, inplace=True)
        query = get_siren_query(temp_siren_list["siren"])
        response = requests.get(
            query,
            headers=headers,
        )
        if response.status_code != 200:
            print("error")
        result = response.json()
        siren_info_list = append_info_siren_list(siren_info_list, result)
    else:
        query = get_siren_query(siren_list["siren"])
        response = requests.get(
            query,
            headers=headers,
        )
        if response.status_code != 200:
            print("error")
        result = response.json()
        siren_info_list = append_info_siren_list(siren_info_list, result)
    return siren_info_list


def save_to_csv(siren_list):
    pd.DataFrame(siren_list).to_csv(f"gcs://{BUCKET_NAME}/{SIREN_FILENAME}")
    return


def siren_to_csv():
    save_to_csv(query_siren())
    return


def siren_to_bq():
    save_to_bq(query_siren())
    return


def save_to_bq(siren_list):
    pd.DataFrame(siren_list).to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.siren_data""",
        project_id=GCP_PROJECT,
        if_exists="append",
    )
    return
