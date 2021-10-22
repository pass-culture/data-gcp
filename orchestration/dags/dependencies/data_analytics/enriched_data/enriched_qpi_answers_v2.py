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


def create_condition(question_id, category):
    return (
        f"SUM(CAST(question_id = '{question_id}' "
        f"and '{FORM[question_id][category]}' IN UNNEST(choices) AS INT64))"
    )


def enrich_answers(
    gcp_project,
    bigquery_clean_dataset,
):
    new_line = ", \n\t     "
    return f"""
        WITH unrolled_answers as (
            SELECT * FROM (
                select *, ROW_NUMBER() OVER() as row_id from `{gcp_project}.{bigquery_clean_dataset}.qpi_answers_v2`
            ) as qpi, qpi.answers as answers
        )

        SELECT max(user_id) as user_id, CAST(max(catch_up_user_id) AS STRING) as catch_up_user_id,
            {
    f'{new_line}'.join(
        [
            f"{create_condition(question_id, category)} > 0 as {category}"
            for question_id in FORM for category in FORM[question_id] ] 
    )
    }
        FROM  unrolled_answers
        group by row_id

    """
