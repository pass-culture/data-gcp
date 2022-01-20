from scripts.import_adage import get_data_adage
from scripts.utils import GCP_PROJECT, BIGQUERY_CLEAN_DATASET


def create_adage_table():
    return f"""
    CREATE TABLE IF NOT EXIST `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.adage`  (
        'id' varchar(250),
        'siret' varchar(250), 
        'regionId' varchar(250), 
        'academieId' varchar(250), 
        'statutId' varchar(250), 
        'labelId' varchar(250),
        'typeId' varchar(250), 
        'communeId' varchar(250), 
        'libelle' varchar(250), 
        'adresse' varchar(250), 
        'siteWeb' varchar(250), 
        'latitude' int(11),
        'longitude' int(11), 
        'actif' int(11), 
        'dateModification' datetime, 
        'statutLibelle' varchar(250),
        'labelLibelle' varchar(250), 
        'typeIcone' varchar(250), 
        'typeLibelle' varchar(250), 
        'communeLibelle' varchar(250),
        'communeDepartement' varchar(250), 
        'academieLibelle' varchar(250), 
        'regionLibelle' varchar(250), 
        'domaines' varchar(250);)
        """



def adding_value():
    keys = get_data_adage()[0]
    values = get_data_adage()[1]
    sql_req = f"INSERT INTO `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.adage` ({}) VALUES {} AS new ON DUPLICATE KEY UPDATE siret = new.siret, regionId = new.regionId".format(
        keys, values
    )
