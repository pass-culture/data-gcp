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


def create_condition(question_id, category, create_variable=True):
    condition = (
        f"SUM(CAST(question_id = '{question_id}' "
        f"and '{FORM[question_id][category]}' IN UNNEST(choices) AS INT64))"
    )
    if not create_variable:
        return condition
    return condition + f" > 0 as {category}"


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

        SELECT max(user_id) as user_id,
            {
    f'{new_line}'.join(
        [
            create_condition(question_id, category)
            for question_id in FORM for category in FORM[question_id] if category != "musique"
        ] + [f"{create_condition('ge0Egr2m8V1T', 'musique', False)} + {create_condition('NeyLJOqShoHw', 'musique')}"]
    )
    }
        FROM  unrolled_answers
        group by row_id

    """
