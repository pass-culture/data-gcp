import hashlib
import hmac
import base64
import os
import pandas as pd
import psycopg2
import sqlalchemy
from google.cloud import secretmanager
from google.auth.exceptions import DefaultCredentialsError

FORM = {
    "NeyLJOqShoHw": {
        "Q0": "regardé un film chez toi 🍿",
        "Q1": "écouté de la musique ♫",
        "Q2": "écouté un podcast 🎧",
        "Q3": "lu un livre 📚",
        "Q4": "lu un article de presse en ligne 📰",
        "Q5": "joué à un jeu vidéo 🎮",
        "Q6": "joué d'un instrument de musique 🎸",
        "Q7": "utilisé du matériel art pour peindre, dessiner... 🎨",
        "Q8": "Aucune de ces activités culturelles",
    },
    "ge0Egr2m8V1T": {
        "Q9": "allé au cinéma 🎞",
        "Q10": "allé à un concert 🤘",
        "Q11": "allé à la bibliothèque, à la médiathèque 📚",
        "Q12": "visité un musée, un monument ou une exposition 🏛",
        "Q13": "assisté à une pièce de théâtre, à un spectacle de cirque, de danse... 💃",
        "Q14": "participé à un festival, à une avant-première 🎷",
        "Q15": "participé à un escape game, à un jeu concours 🎲",
        "Q16": "participé à une conférence, une rencontre ou une découverte de métiers de la Culture 🎤",
        "Q17": "pris un cours de danse, de théâtre, de musique, de dessin... 🎨",
        "Q18": "Aucune de ces sorties culturelles",
    },
    "WiWTxBLGoou4": {
        "Q19": "Théâtre 🎭",
        "Q20": "Spectacle de danse 💃",
        "Q21": "Spectacle d'humour, café théâtre 🎙️",
        "Q22": "Spectacle de rue 🏢",
        "Q23": "Comédie musicale, opéra 👨‍🎤",
        "Q24": "Cirque 🤸",
        "Q25": "Un autre type de spectacle",
    },
    "iX7doTby1OqL": {
        "Q26": "Festival de cinéma 🎬",
        "Q27": "Festival littéraire 📕",
        "Q28": "Festival de musique 🎵",
        "Q29": "Festival de danse, de cirque... 🕺",
        "Q30": "Avant-première de film 🎦",
    },
    ##add new question IDs , with associate responces
}

QPI_TO_SUBCAT = {
    "Q0": ["ABO_MEDIATHEQUE", "AUTRE_SUPPORT_NUMERIQUE", "VOD"],
    "Q1": ["ABO_PLATEFORME_MUSIQUE", "CAPTATION_MUSIQUE", "TELECHARGEMENT_MUSIQUE"],
    "Q2": ["PODCAST"],
    "Q3": ["LIVRE_AUDIO_PHYSIQUE", "TELECHARGEMENT_LIVRE_AUDIO", "LIVRE_PAPIER"],
    "Q4": ["ABO_PRESSE_EN_LIGNE", "APP_CULTURELLE"],
    "Q5": [
        "ACHAT_INSTRUMENT",
        "BON_ACHAT_INSTRUMENT",
        "LOCATION_INSTRUMENT",
        "PARTITION",
    ],
    "Q6": ["ABO_JEU_VIDEO", "ABO_LUDOTHEQUE", "JEU_EN_LIGNE"],
    "Q7": ["MATERIEL_ART_CREATIF"],
    "Q8": [""],
    "Q9": [
        "CARTE_CINE_ILLIMITE",
        "CARTE_CINE_MULTISEANCES",
        "CINE_VENTE_DISTANCE",
        "SEANCE_CINE",
    ],
    "Q10": [""],
    "Q11": ["ABO_CONCERT", "CONCERT", "LIVESTREAM_MUSIQUE"],
    "Q12": ["VISITE", "VISITE_GUIDEE", "VISITE_VIRTUELLE", "EVENEMENT_PATRIMOINE"],
    "Q13": ["ABO_SPECTACLE", "SPECTACLE_ENREGISTRE"],
    "Q14": [""],
    "Q15": ["CONCOURS", "ESCAPE_GAME", "RENCONTRE_JEU", "EVENEMENT_JEU"],
    "Q16": ["CONFERENCE", "DECOUVERTE_METIERS", "RENCONTRE"],
    "Q17": ["ABO_PRATIQUE_ART", "SEANCE_ESSAI_PRATIQUE_ART"],
    "Q18": [""],
    "Q19": ["SPECTACLE_REPRESENTATION"],
    "Q20": ["SPECTACLE_REPRESENTATION"],
    "Q21": ["SPECTACLE_REPRESENTATION"],
    "Q22": ["SPECTACLE_REPRESENTATION"],
    "Q23": ["SPECTACLE_REPRESENTATION"],
    "Q24": ["SPECTACLE_REPRESENTATION"],
    "Q25": ["SPECTACLE_REPRESENTATION"],
    "Q26": ["FESTIVAL_CINE"],
    "Q27": ["FESTIVAL_LIVRE"],
    "Q28": ["FESTIVAL_MUSIQUE"],
    "Q29": ["FESTIVAL_SPECTACLE"],
    "Q30": [""],
}

SUBCAT_LIST = [
    "ABO_BIBLIOTHEQUE",
    "ABO_CONCERT",
    "ABO_JEU_VIDEO",
    "ABO_LIVRE_NUMERIQUE",
    "ABO_LUDOTHEQUE",
    "ABO_MEDIATHEQUE",
    "ABO_MUSEE",
    "ABO_PLATEFORME_MUSIQUE",
    "ABO_PLATEFORME_VIDEO",
    "ABO_PRATIQUE_ART",
    "ABO_PRESSE_EN_LIGNE",
    "ABO_SPECTACLE",
    "ACHAT_INSTRUMENT",
    "ACTIVATION_EVENT",
    "ACTIVATION_THING",
    "APP_CULTURELLE",
    "ATELIER_PRATIQUE_ART",
    "AUTRE_SUPPORT_NUMERIQUE",
    "BON_ACHAT_INSTRUMENT",
    "CAPTATION_MUSIQUE",
    "CARTE_CINE_ILLIMITE",
    "CARTE_CINE_MULTISEANCES",
    "CARTE_MUSEE",
    "CINE_PLEIN_AIR",
    "CINE_VENTE_DISTANCE",
    "CONCERT",
    "CONCOURS",
    "CONFERENCE",
    "DECOUVERTE_METIERS",
    "ESCAPE_GAME",
    "EVENEMENT_CINE",
    "EVENEMENT_JEU",
    "EVENEMENT_MUSIQUE",
    "EVENEMENT_PATRIMOINE",
    "FESTIVAL_CINE",
    "FESTIVAL_LIVRE",
    "FESTIVAL_MUSIQUE",
    "FESTIVAL_SPECTACLE",
    "JEU_EN_LIGNE",
    "JEU_SUPPORT_PHYSIQUE",
    "LIVESTREAM_EVENEMENT",
    "LIVESTREAM_MUSIQUE",
    "LIVRE_AUDIO_PHYSIQUE",
    "LIVRE_NUMERIQUE",
    "LIVRE_PAPIER",
    "LOCATION_INSTRUMENT",
    "MATERIEL_ART_CREATIF",
    "MUSEE_VENTE_DISTANCE",
    "OEUVRE_ART",
    "PARTITION",
    "PODCAST",
    "RENCONTRE_JEU",
    "RENCONTRE",
    "SALON",
    "SEANCE_CINE",
    "SEANCE_ESSAI_PRATIQUE_ART",
    "SPECTACLE_ENREGISTRE",
    "SPECTACLE_REPRESENTATION",
    "SUPPORT_PHYSIQUE_FILM",
    "SUPPORT_PHYSIQUE_MUSIQUE",
    "TELECHARGEMENT_LIVRE_AUDIO",
    "TELECHARGEMENT_MUSIQUE",
    "VISITE_GUIDEE",
    "VISITE_VIRTUELLE",
    "VISITE",
    "VOD",
]


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
        for question_nb in FORM[question_id]:
            if question_nb not in answer_dictionary:
                answer_dictionary[question_nb] = 0
            if "choices" in answer:
                if "labels" in answer["choices"]:
                    answer_dictionary[question_nb] += int(
                        FORM[question_id][question_nb] in answer["choices"]["labels"]
                    )

    answer_values_dictionary = {
        "user_id": answer_dictionary["user_id"],
        "catch_up_user_id": "null",
    }
    NbOfQPIquestions = 31
    qpi_questions = [f"Q{i}" for i in range(NbOfQPIquestions)]
    for qpi_question in qpi_questions:
        answer_values_dictionary[qpi_question] = (
            ["true"] if answer_dictionary.get(qpi_question, 0) > 0 else ["false"]
        )

    ucs = {}
    df_qpi = pd.DataFrame(data=answer_values_dictionary)
    for ind in df_qpi.index:
        user_subcat_list = []
        for question in qpi_questions:
            if df_qpi[f"{question}"][ind] == "true":
                for subcat in QPI_TO_SUBCAT[f"{question}"]:
                    user_subcat_list.append(subcat)
        ucs[f"{df_qpi['user_id'][ind]}"] = {"subcat": list(set(user_subcat_list))}

    final_dict = {}
    for user_id in ucs.keys():
        final_dict["user_id"] = user_id
        for category in SUBCAT_LIST:
            if category in ucs[f"{user_id}"]["subcat"]:
                final_dict[f"{category}"] = True
            else:
                final_dict[f"{category}"] = False
    # Ingest Data
    connection = create_db_connection()

    values_to_insert = (
        f"VALUES ({', '.join([f'{final_dict[key]}' for key in final_dict])})"
    )
    connection.execute(
        f"""INSERT INTO qpi_answers ({', '.join(f'"{key}"' for key in final_dict)}) """
        f"{values_to_insert}"
    )
    return "Success"
