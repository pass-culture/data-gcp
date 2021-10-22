import hashlib
import hmac
import base64
import os

import psycopg2
import sqlalchemy
from google.cloud import secretmanager
from google.auth.exceptions import DefaultCredentialsError

FORM = {
    "ge0Egr2m8V1T": {
        "PRATIQUE_ART": "pris un cours de pratique artistique (danse, théâtre, musique, dessin...) 🎨",
        "AUTRE": "participé à une conférence, une rencontre ou une découverte de métiers de la Culture 🎤",
        "MUSEE": "allé à un musée, une visite ou une exposition  🏛",
        "SPECTACLE": "assisté à une pièce de théâtre, à un spectacle de cirque, de danse... 💃",
        "MUSIQUE_LIVE": "allé à un concert ou un festival 🤘",
        "CINEMA": "allé au cinéma 🎞",
    },
    "NeyLJOqShoHw": {
        "MUSIQUE_ENREGISTREE": "écouté de la musique ♫",
        "INSTRUMENT": "joué de ton instrument de musique 🎸",
        "MEDIA": "lu un article de presse 📰",
        "FILM": "regardé un film chez toi 🍿",
        "JEU": "joué à un jeu vidéo 🎮",
        "LIVRE": "lu un livre 📚",
    },
    "WiWTxBLGoou4": {
        "SPECTACLE": "Théâtre 🎭",
        "SPECTACLE": "Spectacle de danse 💃",
        "SPECTACLE": "Spectacle d'humour, café théâtre 🎙️",
        "SPECTACLE": "Spectacle de rue 🏢",
        "SPECTACLE": "Comédie musicale, opéra 👨‍🎤",
        "SPECTACLE": "Cirque 🤸",
        "SPECTACLE": "Un autre type de spectacle",
    },
    "iX7doTby1OqL": {
        "CINEMA": "Festival de cinéma 🎬",
        "LIVRE": "Festival littéraire 📕",
        "MUSIQUE_LIVE": "Festival de musique 🎵",
        "PRATIQUE_ART": "Festival de danse, de cirque... 🕺",
        "CINEMA": "Avant-première de film 🎦",
    },
    ##add new question IDs , with associate responces
}


def access_secret(project_id, secret_id, version_id=1, default=None):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except DefaultCredentialsError:
        return default


project_name = os.environ["PROJECT_NAME"]
environment = os.environ["ENV"]

# Staging is receiving data from production and staging application (for test purpose)
if environment == "dev":
    FORM["QiK2FlxvTWtK"] = FORM["ge0Egr2m8V1T"]
    FORM["DIsOskUyDgbw"] = FORM["NeyLJOqShoHw"]
    FORM["PuKW507niOgt"] = FORM["WiWTxBLGoou4"]
    FORM["4J8N6fC1aGzh"] = FORM["iX7doTby1OqL"]

    cloud_sql_names_environment = environment

elif environment == "stg":
    FORM["qVZoIyHvj5uu"] = FORM["ge0Egr2m8V1T"]
    FORM["67hKXLLXKMvO"] = FORM["NeyLJOqShoHw"]
    FORM["jwO0vLQzSN5N"] = FORM["WiWTxBLGoou4"]
    FORM["79Zh7dyttVDS"] = FORM["iX7doTby1OqL"]

    # The staging cloudsql database connection name is using 'staging' instead of 'stg'
    cloud_sql_names_environment = "staging"
else:
    cloud_sql_names_environment = environment

TYPEFORM_WEBHOOK_SECRET_KEY = access_secret(
    project_name, f"typeform-webhook-secret-{environment}"
)

db_user = f"cloudsql-recommendation-{environment}"
db_name = f"cloudsql-recommendation-{environment}"
db_pass = access_secret(
    project_name, f"cloudsql-recommendation-{environment}-database-password"
)
db_socket_dir = "/cloudsql"
cloud_sql_connection_name = (
    f"{project_name}:europe-west1:cloudsql-recommendation-{cloud_sql_names_environment}"
)

url = sqlalchemy.engine.url.URL(
    drivername="postgresql+pg8000",
    username=db_user,
    password=db_pass,
    database=db_name,
    query={
        "unix_sock": "{}/{}/.s.PGSQL.5432".format(
            db_socket_dir, cloud_sql_connection_name
        )
    },
)
engine = sqlalchemy.create_engine(url)


def create_db_connection():
    return engine.connect().execution_options(autocommit=True)


def run(request):
    """The Cloud Function entrypoint."""

    # Authentification
    header_signature = request.headers.get("Typeform-Signature")
    if not header_signature:
        raise ValueError("No Signature Provided.")

    sha_name, signature = header_signature.split("=", 1)
    if sha_name != "sha256":
        raise ValueError("Wrong Signature Encoding")

    mac = hmac.new(
        bytearray(TYPEFORM_WEBHOOK_SECRET_KEY, encoding="utf-8"),
        msg=bytearray(request.get_data()),
        digestmod=hashlib.sha256,
    )
    if not hmac.compare_digest(
        bytearray(base64.b64encode(mac.digest()).decode(), encoding="utf-8"),
        bytearray(signature, encoding="utf_8"),
    ):
        raise ValueError("Wrong Signature Value.")

    # Reshape Data
    request_json = request.get_json()
    answer_dictionary = {
        "user_id": f"'{request_json['form_response']['hidden']['userpk']}'",
    }
    answers = request_json["form_response"]["answers"]
    for answer in answers:
        question_id = answer["field"]["id"]
        for category in FORM[question_id]:
            if category not in answer_dictionary:
                answer_dictionary[category] = 0
            if "choices" in answer:
                if "labels" in answer["choices"]:
                    answer_dictionary[category] += int(
                        FORM[question_id][category] in answer["choices"]["labels"]
                    )

    answer_values_dictionary = {
        "user_id": answer_dictionary["user_id"],
        "catch_up_user_id": "null",
    }
    for category in [
        "BEAUX_ARTS",
        "CINEMA",
        "CONFERENCE_RENCONTRE",
        "FILM",
        "INSTRUMENT",
        "JEU",
        "LIVRE",
        "MEDIA",
        "MUSEE",
        "MUSIQUE_ENREGISTREE",
        "MUSIQUE_LIVE",
        "PRATIQUE_ART",
        "SPECTACLE",
        "TECHNIQUE",
    ]:
        answer_values_dictionary[category] = (
            "true" if answer_dictionary.get(category, 0) > 0 else "false"
        )

    # Ingest Data
    connection = create_db_connection()

    connection.execute(
        f"INSERT INTO qpi_answers ({', '.join([key for key in answer_values_dictionary])}) "
        f"VALUES ({', '.join([answer_values_dictionary[key] for key in answer_values_dictionary])})"
    )
    return "Success"
