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


def create_condition(question_id, question_nb):
    return (
        f"SUM(CAST(question_id = '{question_id}' "
        f"""and "{FORM[question_id][question_nb]}" IN UNNEST(choices) AS INT64))"""
    )


def enrich_answers(
    gcp_project,
    bigquery_clean_dataset,
):
    if bigquery_clean_dataset == "clean_dev":
        FORM["QiK2FlxvTWtK"] = FORM["ge0Egr2m8V1T"]
        FORM["DIsOskUyDgbw"] = FORM["NeyLJOqShoHw"]
        FORM["PuKW507niOgt"] = FORM["WiWTxBLGoou4"]
        FORM["4J8N6fC1aGzh"] = FORM["iX7doTby1OqL"]
        FORM.pop("ge0Egr2m8V1T")
        FORM.pop("NeyLJOqShoHw")
        FORM.pop("WiWTxBLGoou4")
        FORM.pop("iX7doTby1OqL")

    elif bigquery_clean_dataset == "clean_stg":
        FORM["qVZoIyHvj5uu"] = FORM["ge0Egr2m8V1T"]
        FORM["67hKXLLXKMvO"] = FORM["NeyLJOqShoHw"]
        FORM["jwO0vLQzSN5N"] = FORM["WiWTxBLGoou4"]
        FORM["79Zh7dyttVDS"] = FORM["iX7doTby1OqL"]
        FORM.pop("ge0Egr2m8V1T")
        FORM.pop("NeyLJOqShoHw")
        FORM.pop("WiWTxBLGoou4")
        FORM.pop("iX7doTby1OqL")

    new_line = ", \n\t     "
    return f"""
        WITH unrolled_answers as (
            SELECT * FROM (
                select *, ROW_NUMBER() OVER() as row_id from `{gcp_project}.{bigquery_clean_dataset}.qpi_answers_v3`
            ) as qpi, qpi.answers as answers
        )

        SELECT max(user_id) as user_id, CAST(max(catch_up_user_id) AS STRING) as catch_up_user_id,
            {
        f'{new_line}'.join(
        [f"CASE WHEN ({create_condition(question_id, question_nb)} > 0)  then { [f'{tag}' for tag in QPI_TO_SUBCAT[question_nb]]} else NULL END  as {question_nb}"
            for question_id in FORM for question_nb in FORM[question_id] ] 
            )
            }
        FROM  unrolled_answers
        group by row_id

    """
