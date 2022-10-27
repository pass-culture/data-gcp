from datetime import datetime
from scripts.utils import (
    GCP_PROJECT,
    ENV_SHORT_NAME,
    BIGQUERY_ANALYTICS_DATASET,
    BUCKET_NAME,
    ADAGE_INVOLVED_STUDENTS_DTYPE,
    save_to_raw_bq,
)

from google.cloud import secretmanager
from google.auth.exceptions import DefaultCredentialsError
import requests
import os
import pandas as pd


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

        req = requests.get(
            "{}/{}".format(ENDPOINT, route),
            headers=headers,
        )
        if req.status_code == 200:
            data = req.json()
            return data
    except Exception as e:
        print("An unexpected error has happened {}".format(e))
    return None


def create_adage_table():
    return f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.adage`(
        id STRING,
        siret STRING,
        venueId STRING,  
        regionId STRING, 
        academieId STRING, 
        statutId STRING, 
        labelId STRING,
        typeId STRING, 
        communeId STRING, 
        libelle STRING, 
        adresse STRING, 
        siteWeb STRING, 
        latitude STRING,
        longitude STRING, 
        actif STRING, 
        dateModification STRING, 
        statutLibelle STRING,
        labelLibelle STRING, 
        typeIcone STRING, 
        typeLibelle STRING, 
        communeLibelle STRING,
        communeDepartement STRING, 
        academieLibelle STRING, 
        regionLibelle STRING, 
        domaines STRING);
        """


def adding_value():
    return f"""MERGE `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.adage` A
        USING `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.adage_data_temp` B
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
    }
    ids = [k for k, v in adage_ids.items() if (_now > v[0] and _now <= v[1])]

    export = []
    for _id in ids:

        results = get_request(ENDPOINT, API_KEY, route=f"stats-pass-culture/{_id}")
        for metric_name, rows in results.items():
            for metric_id, v in rows.items():
                export.append(
                    dict(
                        {
                            "metric_name": metric_name,
                            "metric_id": metric_id,
                            "educational_year_adage_id": _id,
                            "metric_key": v[stats_dict[metric_name]],
                            "involved_students": v["eleves"],
                            "institutions": v["etabs"],
                            "total_involved_students": v["totalEleves"],
                            "total_institutions": v["totalEtabs"],
                        },
                    )
                )

    df = pd.DataFrame(export)
    # force types
    for k, v in ADAGE_INVOLVED_STUDENTS_DTYPE.items():
        df[k] = df[k].astype(v)
    save_to_raw_bq(df, "adage_involved_student")
