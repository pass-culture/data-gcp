from utils import (
    API_KEY,
    ENDPOINT,
    GCP_PROJECT,
    ENV_SHORT_NAME,
    BIGQUERY_CLEAN_DATASET,
    BUCKET_NAME,
)
import requests


def get_partenaire_culturel():
    try:
        headers = {"X-omogen-api-key": API_KEY}

        req = requests.get(
            "{}/partenaire-culturel".format(ENDPOINT),
            headers=headers,
        )
        if req.status_code == 200:
            data = req.json()
            return data
    except Exception as e:
        print("An unexpected error has happened {}".format(e))
    return None


def get_data_adage():
    datas = get_partenaire_culturel()
    print(datas)
    keys = ",".join(list(datas[0].keys()))
    values = ", ".join(
        [
            "({})".format(
                " , ".join(
                    [
                        "'{}'".format(d[k]) if d[k] is not None else "NULL"
                        for k in list(d.keys())
                    ]
                )
            )
            for d in datas
        ]
    )
    return keys, values


def create_adage_table():
    return f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.adage`(
        id STRING,
        siret STRING, 
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


def adding_value(datas, i):
    d1 = list(dict(datas[i]).values())
    d2 = ["None" if v is None else v for v in d1]
    return (
        f"""INSERT INTO `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.adage` """
        + """ ({}) VALUES {};""".format(
            ",".join(map(str, list((((dict(datas[i]).keys())))))), tuple(d2)
        )
    )


def save_adage_to_bq(datas, i):
    return adding_value(datas, i)
