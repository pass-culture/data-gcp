from scripts.utils import (
    API_KEY,
    GCP_PROJECT,
    ENV_SHORT_NAME,
    BIGQUERY_CLEAN_DATASET,
    BUCKET_NAME,
)
import requests
import os


def get_endpoint():
    if os.environ["ENV_SHORT_NAME"] == "prod":
        return "https://omogen-api-pr.phm.education.gouv.fr/adage-api/v1"

    elif os.environ["ENV_SHORT_NAME"] == "stg":
        return "https://omogen-api-pr.phm.education.gouv.fr/adage-api-staging/v1"
    else:
        return "https://omogen-api-pr.phm.education.gouv.fr/adage-api-test/v1"


def get_partenaire_culturel():
    ENDPOINT = get_endpoint()
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


def adding_value():
    return f"""MERGE `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.adage` A
        USING `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.adage_data_temp` B
        ON B.id = A.id
        WHEN MATCHED THEN
            UPDATE SET 
            siret = B.siret, 
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
        INSERT (id,siret,regionId,academieId,
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
                domaines) VALUES(id,siret,regionId,academieId, statutId,
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
                                 domaines)"""
