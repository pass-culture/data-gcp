def return_query(
    gcp_project,
    bigquery_raw_dataset,
    bigquery_clean_dataset,
    bigquery_analytics_dataset,
):
    return f"""
        WITH unrolled_answers as (
            SELECT * FROM `{gcp_project}.{bigquery_raw_dataset}.qpi_answers_v2` as qpi, qpi.answers as answers
        ),
        question1 as (
            SELECT culturalsurvey_id as id, user_id,
            "pris un cours de pratique artistique (danse, théâtre, musique, dessin...) 🎨" IN UNNEST(choices) as pratique_artistique,
            "participé à une conférence, une rencontre ou une découverte de métiers de la Culture 🎤" IN UNNEST(choices) as autre,
            "allé à un musée, une visite ou une exposition  🏛" IN UNNEST(choices) as musees_patrimoine,
            "assisté à une pièce de théâtre, à un spectacle de cirque, de danse... 💃" IN UNNEST(choices) as spectacle_vivant,
            "allé à un concert ou un festival 🤘" IN UNNEST(choices) as musique,
            "allé au cinéma 🎞" IN UNNEST(choices) as cinema
            FROM  unrolled_answers
            WHERE question_id = "ge0Egr2m8V1T"
        ),
        question2 as (
            SELECT culturalsurvey_id as id, user_id,
            "joué de ton instrument de musique 🎸" IN UNNEST(choices) as instrument,
            "lu un article de presse 📰" IN UNNEST(choices) as presse,
            "regardé un film chez toi 🍿" IN UNNEST(choices) as audiovisuel,
            "joué à un jeu vidéo 🎮" IN UNNEST(choices) as jeux_videos,
            "écouté de la musique ♫" IN UNNEST(choices) as musique,
            "lu un livre 📚" IN UNNEST(choices) as livre
            FROM  unrolled_answers
            WHERE question_id = "NeyLJOqShoHw"
        ),
        users AS (
            SELECT user_id, user_cultural_survey_id, user_civility, user_activity, FROM `{gcp_project}.{bigquery_clean_dataset}.applicative_database_user`
        ),
        enriched_users as (
            SELECT user_id, booking_cnt as booking_count, first_connection_date FROM `{gcp_project}.{bigquery_analytics_dataset}.enriched_user_data`
        )

        select (CASE question1.user_id WHEN null THEN users.user_id else question1.user_id END) as user_id, 
        user_civility, users.user_activity,
        booking_count, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),  CAST(first_connection_date AS TIMESTAMP), DAY) as days_since_first_connection,
        pratique_artistique, autre, musees_patrimoine, spectacle_vivant, cinema, instrument, presse, audiovisuel, jeux_videos, livre,
        IFNULL(question1.musique is true or question2.musique is true, false) as musique,
        FROM question1
        INNER JOIN question2
        on question1.id = question2.id
        LEFT JOIN users
        on question1.id = users.user_cultural_survey_id
        LEFT JOIN enriched_users
        on users.user_id = enriched_users.user_id
    """
