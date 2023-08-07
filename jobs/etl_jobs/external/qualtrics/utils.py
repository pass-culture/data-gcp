import os

from google.auth.exceptions import DefaultCredentialsError
from google.cloud import secretmanager
from google.cloud import bigquery

from datetime import datetime

PROJECT_NAME = os.environ.get("PROJECT_NAME")
BIGQUERY_RAW_DATASET = os.environ.get("BIGQUERY_RAW_DATASET")
ENVIRONMENT_SHORT_NAME = os.environ.get("ENVIRONMENT_SHORT_NAME")


def access_secret_data(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


def save_to_raw_bq(df, table_name, schema):
    _now = datetime.today()
    yyyymmdd = _now.strftime("%Y%m%d")
    df["execution_date"] = _now
    bigquery_client = bigquery.Client()
    table_id = f"{PROJECT_NAME}.{BIGQUERY_RAW_DATASET}.{table_name}${yyyymmdd}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="execution_date",
        ),
        schema=[
            bigquery.SchemaField(column, _type) for column, _type in schema.items()
        ],
    )
    job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()


API_TOKEN = access_secret_data(
    PROJECT_NAME, f"qualtrics_token_{ENVIRONMENT_SHORT_NAME}"
)
DATA_CENTER = access_secret_data(
    PROJECT_NAME, f"qualtrics_data_center_{ENVIRONMENT_SHORT_NAME}"
)
DIRECTORY_ID = access_secret_data(
    PROJECT_NAME, f"qualtrics_directory_id_{ENVIRONMENT_SHORT_NAME}"
)

OPT_OUT_EXPORT_COLUMNS = {
    "contactId": "contact_id",
    "firstName": "first_name",
    "lastName": "last_name",
    "email": "email",
    "phone": "phone",
    "language": "language",
    "extRef": "ext_ref",
    "directoryUnsubscribed": "directory_unsubscribed",
    "directoryUnsubscribeDate": "directory_unsubscribe_date",
}

IR_JEUNES_TABLE_SHEMA = {
    "StartDate": "STRING",
    "EndDate": "STRING",
    "Status": "STRING",
    "IPAddress": "STRING",
    "Progress": "STRING",
    "Duration_in_seconds": "STRING",
    "Finished": "STRING",
    "RecordedDate": "STRING",
    "ResponseId": "STRING",
    "ExternalReference": "STRING",
    "LocationLatitude": "STRING",
    "LocationLongitude": "STRING",
    "DistributionChannel": "STRING",
    "UserLanguage": "STRING",
    "Q1_NPS_GROUP": "STRING",
    "account_created": "STRING",
    "nb_jours_connexion_last_month": "STRING",
    "nb_resas_non_annulees": "STRING",
    "nb_reservations_reseaux": "STRING",
    "theoretical_amount_spent": "STRING",
    "user_activity": "STRING",
    "user_civility": "STRING",
    "user_department_code": "STRING",
    "user_deposit_creation_date": "STRING",
    "user_in_qpv": "STRING",
    "user_in_ZRR": "STRING",
    "user_region_name": "STRING",
    "user_seniority": "STRING",
    "actual_amount_spent": "STRING",
    "anciennete_en_jours": "STRING",
    "civility": "STRING",
    "code_qpv": "STRING",
    "cohorte": "STRING",
    "departement": "STRING",
    "epci_name": "STRING",
    "geo_type": "STRING",
    "Les_deux": "STRING",
    "Lieu_de_réservation": "STRING",
    "nb_visit_le_dernier_mois": "STRING",
    "offer_id": "STRING",
    "offer_name": "STRING",
    "region": "STRING",
    "reservation_jep": "STRING",
    "total_reservation_non_annulees": "STRING",
    "type_jeune": "STRING",
    "type_utilisateur": "STRING",
    "user_age": "STRING",
    "zrr": "STRING",
    "Campagne": "STRING",
    "no_cancelled_booking": "STRING",
    "Q3_Sentiment": "INTEGER",
    "Q3_Sentiment_Polarity": "INTEGER",
    "Q3_Sentiment_Score": "STRING",
    "Q3_Topics": "INTEGER",
    "Q3_Parent_Topics": "INTEGER",
    "Q3_Topic_Sentiment_Label": "INTEGER",
    "Q3_Topic_Sentiment_Score": "INTEGER",
    "Nombre_de_jours_de_connexion_au_cours_du_dernier_mois": "STRING",
    "Nombre_de_réservations": "STRING",
    "Nb_de_résa_justifié": "STRING",
    "Nombre_de_réservations_V2": "STRING",
    "Montant_dépensé": "STRING",
    "Ancienneté": "STRING",
    "question": "STRING",
    "answer": "STRING",
    "question_str": "STRING",
    "question_id": "STRING",
    "user_type": "STRING",
}

IR_PRO_TABLE_SCHEMA = {
    "StartDate": "STRING",
    "EndDate": "STRING",
    "Status": "STRING",
    "IPAddress": "STRING",
    "Progress": "STRING",
    "Duration_in_seconds": "STRING",
    "Finished": "STRING",
    "RecordedDate": "STRING",
    "ResponseId": "STRING",
    "ExternalReference": "STRING",
    "LocationLatitude": "STRING",
    "LocationLongitude": "STRING",
    "DistributionChannel": "STRING",
    "UserLanguage": "STRING",
    "Q1_NPS_GROUP": "STRING",
    "anciennete_jours": "STRING",
    "non_cancelled_bookings": "STRING",
    "offers_created": "STRING",
    "theoretic_revenue": "STRING",
    "venue_department_code": "STRING",
    "venue_is_permanent": "STRING",
    "venue_name": "STRING",
    "venue_region_name": "STRING",
    "venue_type_label": "STRING",
    "Campagne": "STRING",
    "nb_jours_depuis_derniere_resa": "STRING",
    "anciennete_en_jours": "STRING",
    "collective_offers_created": "STRING",
    "individual_offers_created": "STRING",
    "last_application_status": "STRING",
    "Avis": "INTEGER",
    "Date_v2": "INTEGER",
    "Date_v3": "STRING",
    "Nombre_de_jours_sans_réservation_V2": "STRING",
    "Q2_Actionability": "STRING",
    "Q1_Parent_Topics": "INTEGER",
    "Q1_Sentiment_Polarity": "STRING",
    "Q1_Sentiment_Score": "STRING",
    "Q1_Sentiment": "STRING",
    "Q1_Topic_Sentiment_Label": "STRING",
    "Q1_Topic_Sentiment_Score": "STRING",
    "Q1_Topics": "STRING",
    "Ancienneté_v2": "STRING",
    "Nombre_de_jours_dernière_résa": "STRING",
    "Nombre_de_réservation": "STRING",
    "question": "STRING",
    "answer": "STRING",
    "question_str": "STRING",
    "question_id": "STRING",
    "user_type": "STRING",
}
