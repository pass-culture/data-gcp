import logging
from datetime import datetime

import pandas as pd
import pandas_gbq
import requests
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager

from scripts.utils import (
    ADAGE_INVOLVED_STUDENTS_DTYPE,
    BIGQUERY_ANALYTICS_DATASET,
    BIGQUERY_RAW_DATASET,
    BQ_ADAGE_DTYPE,
    ENV_SHORT_NAME,
    GCP_PROJECT,
    RequestReturnedNoneError,
    save_to_raw_bq,
)

logger = logging.getLogger(__name__)


def get_endpoint():
    if ENV_SHORT_NAME == "prod":
        return "https://omogen-api-pr.phm.education.gouv.fr/adage-api/v1"

    elif ENV_SHORT_NAME == "stg":
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
project_name = GCP_PROJECT


if ENV_SHORT_NAME == "dev":
    API_KEY = access_secret(project_name, "adage_import_api_key")
elif ENV_SHORT_NAME == "stg":
    API_KEY = access_secret(project_name, "adage_import_api_key_stg")
else:
    API_KEY = access_secret(project_name, "adage_import_api_key_prod")


def get_request(ENDPOINT, API_KEY, route, params={}):
    url = "{}/{}".format(ENDPOINT, route)
    logger.info("GET %s params=%s", url, params)
    try:
        headers = {"X-omogen-api-key": API_KEY}
        req = requests.get(url, headers=headers, params=params, timeout=120)
        if req.status_code == 200:
            data = req.json()
            logger.info(
                "GET %s -> HTTP 200, %d records",
                url,
                len(data) if isinstance(data, list) else 1,
            )
            return data
        else:
            logger.error("GET %s -> HTTP %d", url, req.status_code)
    except Exception as e:
        logger.error("GET %s -> unexpected error: %s", url, e)
    return None


def import_adage(since_date):
    logger.info("Creating historical table if not exists")
    client = bigquery.Client()
    client.query(create_adage_historical_table()).result()

    params = {"dateModificationMin": since_date.strftime("%Y-%m-%d %H:%M:%S")}
    data = get_request(ENDPOINT, API_KEY, route="partenaire-culturel", params=params)

    if data is None:
        raise RequestReturnedNoneError(
            "Adage API returned None for endpoint partenaire-culturel"
        )

    df = pd.DataFrame(data)
    logger.info("Fetched %d partner records from API", len(df))
    df["update_date"] = datetime.now().strftime("%Y-%m-%d")
    _cols = list(df.columns)
    for k, v in BQ_ADAGE_DTYPE.items():
        if k not in _cols:
            df[k] = None
        df[k] = df[k].astype(str)

    logger.info("Writing %d rows to BQ table %s.adage", len(df), BIGQUERY_RAW_DATASET)
    pandas_gbq.to_gbq(
        df,
        f"{BIGQUERY_RAW_DATASET}.adage",
        project_id=GCP_PROJECT,
        if_exists="append",
    )
    logger.info("Running MERGE into adage_historical")
    client.query(adding_value()).result()
    logger.info("MERGE into adage_historical completed")


def create_adage_historical_table():
    str_dtype = ",".join([f"{k} {v}" for k, v in BQ_ADAGE_DTYPE.items()])
    return f"""CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.adage_historical`({str_dtype});"""


def adding_value():
    return f"""MERGE `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.adage_historical` A
        USING (SELECT * FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.adage` qualify row_number() over (partition by id order by update_date desc) = 1) B
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
    logger.info("Collecting stats for educational year IDs: %s", ids)

    export = []
    for _id in ids:
        logger.info("Fetching stats for educational year %d", _id)
        results = get_request(ENDPOINT, API_KEY, route=f"stats-pass-culture/{_id}")

        if results is None:
            raise RequestReturnedNoneError(
                f"Adage API returned None for endpoint stats-pass-culture/{_id}"
            )

        rows_before = len(export)
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
                                    "involved_students": v["niveaux"][level]["eleves"],
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
        logger.info(
            "Educational year %d -> %d stat rows collected",
            _id,
            len(export) - rows_before,
        )

    logger.info(
        "Total stat rows: %d, writing to BQ table adage_involved_student", len(export)
    )
    df = pd.DataFrame(export)
    # force types
    for k, v in ADAGE_INVOLVED_STUDENTS_DTYPE.items():
        df[k] = df[k].astype(v)
    save_to_raw_bq(df, "adage_involved_student")
    logger.info("Stats saved to BQ successfully")
