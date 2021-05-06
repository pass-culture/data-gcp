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
        "pratique_artistique": "pris un cours de pratique artistique (danse, théâtre, musique, dessin...) 🎨",
        "autre": "participé à une conférence, une rencontre ou une découverte de métiers de la Culture 🎤",
        "musees_patrimoine": "allé à un musée, une visite ou une exposition  🏛",
        "spectacle_vivant": "assisté à une pièce de théâtre, à un spectacle de cirque, de danse... 💃",
        "musique": "allé à un concert ou un festival 🤘",
        "cinema": "allé au cinéma 🎞",
    },
    "NeyLJOqShoHw": {
        "musique": "écouté de la musique ♫",
        "instrument": "joué de ton instrument de musique 🎸",
        "presse": "lu un article de presse 📰",
        "audiovisuel": "regardé un film chez toi 🍿",
        "jeux_videos": "joué à un jeu vidéo 🎮",
        "livre": "lu un livre 📚",
    },
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
if environment == "stg":
    FORM["ApKEvHgWIs0D"] = FORM["ge0Egr2m8V1T"]
    FORM["zg4ydcgaiF6a"] = FORM["NeyLJOqShoHw"]
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
        "cinema",
        "audiovisuel",
        "jeux_videos",
        "livre",
        "musees_patrimoine",
        "musique",
        "pratique_artistique",
        "spectacle_vivant",
        "instrument",
        "presse",
        "autre",
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
