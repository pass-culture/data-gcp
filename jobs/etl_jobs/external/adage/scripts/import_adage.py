import os
from datetime import datetime
import logging
logger = logging.getLogger(__name__)

import pandas as pd
import requests
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager

from scripts.utils import (
    ADAGE_INVOLVED_STUDENTS_DTYPE,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_RAW_DATASET,
    BQ_ADAGE_DTYPE,
    GCP_PROJECT,
    RequestReturnedNoneError,
    save_to_raw_bq,
)


def get_endpoint():
    if os.environ["ENV_SHORT_NAME"] == "prod":
        return "https://omogen-api-pr.phm.education.gouv.fr/adage-api/v1"

    elif os.environ["ENV_SHORT_NAME"] == "stg":
        return "https://omogen-api-tst-pr.phm.education.gouv.fr/adage-api-staging/v1"
    else:
        return "https://omogen-api-tst-pr.phm.education.gouv.fr/adage-api-test/v1"


def access_secret(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


ENDPOINT = get_endpoint()
project_name = os.environ["PROJECT_NAME"]


if os.environ["ENV_SHORT_NAME"] == "dev":
    API_KEY = access_secret(project_name, "adage_import_api_key")
elif os.environ["ENV_SHORT_NAME"] == "stg":
    API_KEY = access_secret(project_name, "adage_import_api_key_stg")
else:
    API_KEY = access_secret(project_name, "adage_import_api_key_prod")


def get_request(ENDPOINT, API_KEY, route):
    try:
        headers = {"X-omogen-api-key": API_KEY}

        req = requests.get(f"{ENDPOINT}/{route}", headers=headers)
        if req.status_code == 200:
            if not req.content:
                logger.info(f"Empty response for route {route}")
                return {}
            try:
                return req.json()
            except requests.exceptions.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON for route {route}: {e}")
                return None
        else:
            logger.error(
                f"Request to {route} failed with status code {req.status_code}: {req.text}"
            )
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"An unexpected error has happened with route {route}: {e}")
        return None


def import_adage():
    client = bigquery.Client()
    client.query(create_adage_historical_table()).result()
    data = get_request(ENDPOINT, API_KEY, route="partenaire-culturel")

    if data is None:
        raise RequestReturnedNoneError(
            "Adage API returned None for endpoint partenaire-culturel"
        )
    else:
        df = pd.DataFrame(data)
        _cols = list(df.columns)
        for k, v in BQ_ADAGE_DTYPE.items():
            if k not in _cols:
                df[k] = None
            df[k] = df[k].astype(str)

        df.to_gbq(
            f"""{BIGQUERY_RAW_DATASET}.adage""",
            project_id=GCP_PROJECT,
            if_exists="replace",
        )
        client.query(adding_value()).result()


def create_adage_historical_table():
    str_dtype = ",".join([f"{k} {v}" for k, v in BQ_ADAGE_DTYPE.items()])
    return f"""CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.adage_historical`({str_dtype});"""


def adding_value():
    return f"""MERGE `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.adage_historical` A
        USING `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.adage` B
        ON B.id = A.id
        WHEN MATCHED THEN
            UPDATE SET
            siret = B.siret,
            venueId = B.venueId,
            regionId = B.regionId,
            academieId = B.academieId,
            statutId = B.statutId ,
            labelId = B.labelId,
            typeId = B.typeId,
            communeId = B.communeId,
            libelle = B.libelle,
            adresse = B.adresse,
            siteWeb = B.siteWeb,
            latitude = B.latitude,
            longitude = B.longitude,
            actif = B.actif,
            dateModification = B.dateModification,
            statutLibelle = B.statutLibelle,
            labelLibelle = B.labelLibelle,
            typeIcone = B.typeIcone,
            typeLibelle = B.typeLibelle,
            communeLibelle = B.communeLibelle,
            communeDepartement = B.communeDepartement,
            academieLibelle = B.academieLibelle,
            regionLibelle = B.regionLibelle,
            domaines = B.domaines
        WHEN NOT MATCHED THEN
        INSERT (id,siret,venueId,regionId,academieId,
                statutId,
                labelId,
                typeId,
                communeId,
                libelle,
                adresse,
                siteWeb,
                latitude,
                longitude,
                actif,
                dateModification,
                statutLibelle,
                labelLibelle,
                typeIcone,
                typeLibelle,
                communeLibelle,
                communeDepartement,
                academieLibelle,
                regionLibelle,
                domaines) VALUES(id,siret,venueId,regionId,academieId, statutId,labelId,
                                 typeId,
                                 communeId,
                                 libelle,
                                 adresse,
                                 siteWeb,
                                 latitude,
                                 longitude,
                                 actif,
                                 dateModification,
                                 statutLibelle,
                                 labelLibelle,
                                 typeIcone,
                                 typeLibelle,
                                 communeLibelle,
                                 communeDepartement,
                                 academieLibelle,
                                 regionLibelle,
                                 domaines)"""


def get_adage_stats():
    _now = datetime.now().strftime("%Y-%m-%d")
    stats_dict = {
        "departements": "departement",
        "academies": "academie",
        "regions": "region",
    }
    adage_ids = {
        7: ("2021-09-01", "2022-09-10"),
        8: ("2022-09-01", "2023-09-10"),
        9: ("2023-09-01", "2024-09-10"),
        10: ("2024-09-01", "2025-09-10"),
        11: ("2025-09-01", "2026-09-10"),
    }
    ids = [k for k, v in adage_ids.items() if (_now > v[0] and _now <= v[1])]

    export = []
    for _id in ids:
        results = get_request(ENDPOINT, API_KEY, route=f"stats-pass-culture/{_id}")

        if results is None:
            raise RequestReturnedNoneError(
                f"Adage API returned None for endpoint stats-pass-culture/{_id}"
            )
        else:
            for metric_name, rows in results.items():
                for metric_id, v in rows.items():
                    if v["niveaux"] != []:
                        for level in v["niveaux"].keys():
                            export.append(
                                dict(
                                    {
                                        "metric_name": metric_name,
                                        "metric_id": metric_id,
                                        "educational_year_adage_id": _id,
                                        "metric_key": v[stats_dict[metric_name]],
                                        "level": level,
                                        "involved_students": v["niveaux"][level][
                                            "eleves"
                                        ],
                                        "institutions": v["etabs"],
                                        "total_involved_students": v["niveaux"][level][
                                            "totalEleves"
                                        ],
                                        "total_institutions": v["totalEtabs"],
                                    }
                                )
                            )
                    else:
                        export.append(
                            dict(
                                {
                                    "metric_name": metric_name,
                                    "metric_id": metric_id,
                                    "educational_year_adage_id": _id,
                                    "metric_key": v[stats_dict[metric_name]],
                                    "level": None,
                                    "involved_students": v["eleves"],
                                    "institutions": v["etabs"],
                                    "total_involved_students": v["totalEleves"],
                                    "total_institutions": v["totalEtabs"],
                                }
                            )
                        )

    if not export:
        logger.info("No stats to import from Adage API.")
        return

    df = pd.DataFrame(export)
    # force types
    for k, v in ADAGE_INVOLVED_STUDENTS_DTYPE.items():
        if k in df.columns:
            df[k] = df[k].astype(v)
    save_to_raw_bq(df, "adage_involved_student")
