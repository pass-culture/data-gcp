-- enriched_qpi_answers_v1
WITH unrolled_answers as (
    SELECT * FROM `passculture-data-ehp.raw_stg.qpi_answers_v1` as qpi, qpi.answers as answers
),
pratiques as (
    SELECT culturalsurvey_id as id,
    "🎞 FILM & SÉRIE" IN UNNEST(choices) as film,
    "🎮 JEU VIDÉO" IN UNNEST(choices) as jeux_video,
    "📚 LECTURE" IN UNNEST(choices) as lecture,
    "🏛 VISITE — musée, expo, monument..." IN UNNEST(choices) as visite,
    "🤘 FESTIVAL" IN UNNEST(choices) as festival,
    "♫ MUSIQUE — écoute, concert" IN UNNEST(choices) as musique,
    "🎸 PRATIQUE ARTISTIQUE — danse, instrument, écriture, dessin..." IN UNNEST(choices) as pratique_artistique,
    "💃 SPECTACLE — théâtre, cirque, danse..." IN UNNEST(choices) as spectacle
    FROM  unrolled_answers
    WHERE question_id = "XojHzweFHSEj"
),
vod as (
    SELECT culturalsurvey_id as id FROM unrolled_answers where question_id = "dZmbwWSzroeN" and ARRAY_LENGTH(choices) > 0
),
cinema as (
    SELECT culturalsurvey_id as id FROM unrolled_answers where question_id = "Vo0aiAJsoymf" and choice != 'Jamais'
),
concerts as (
    SELECT culturalsurvey_id as id
    FROM unrolled_answers where question_id = "aOKS8s9Ssded" and choice != 'Aucune'
),
supports_musique as (
    SELECT culturalsurvey_id as id,
    ('Physique — CD, vinyle...' in UNNEST(choices)) as physique,
    EXISTS(SELECT 1 FROM UNNEST(choices) choice WHERE choice IN (
        'Numérique  — MP3 ou autre',
        'Plateforme(s) de streaming spécialisée(s) — Deezer, Spotify...',
        'Autre(s) plateforme(s) de streaming — Youtube, Dailymotion...'
    )) as numerique,
    FROM unrolled_answers where question_id = "YLjUuafoEfi4"
),
pratique_musique as (
    SELECT culturalsurvey_id as id
    FROM unrolled_answers where question_id = "MxgbTe4j5Iee" and 'Faire de la musique ou du chant' in UNNEST(choices)
),
users AS (
    SELECT user_id, user_cultural_survey_id, user_civility, user_activity, FROM `passculture-data-ehp.clean_stg.applicative_database_user`
),
enriched_users as (
    SELECT user_id, booking_cnt as booking_count, first_connection_date FROM `passculture-data-ehp.analytics_stg.enriched_user_data`
)

select users.user_id, user_civility, users.user_activity,
booking_count, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),  CAST(first_connection_date AS TIMESTAMP), DAY) as days_since_first_connection,
IFNULL(culturalsurvey_id IN (select id from pratiques where film is true) and culturalsurvey_id in (select id from vod), false) as audiovisuel,
IFNULL(culturalsurvey_id IN (select id from pratiques where film is true) and culturalsurvey_id in (select id from cinema), false) as cinema,
IFNULL(culturalsurvey_id IN (select id from pratiques where jeux_video is true), false) as jeux_videos,
IFNULL(culturalsurvey_id IN (select id from pratiques where lecture is true), false) as livre,
IFNULL(culturalsurvey_id IN (select id from pratiques where visite is true), false) as musees_patrimoine,
IFNULL(culturalsurvey_id IN (select id from pratiques where musique is true), false) as musique,
IFNULL(culturalsurvey_id IN (select id from pratiques where pratique_artistique is true), false) as pratique_artistique,
IFNULL(culturalsurvey_id IN (select id from pratiques where spectacle is true), false) as spectacle_vivant,
IFNULL(culturalsurvey_id IN (select id from pratiques where pratique_artistique is true) and culturalsurvey_id IN (select id from pratique_musique), false) as instrument,
FROM `passculture-data-ehp.raw_stg.qpi_answers_v1` answers
LEFT JOIN users
on answers.culturalsurvey_id = users.user_cultural_survey_id
LEFT JOIN enriched_users
on users.user_id = enriched_users.user_id

-- enriched_qpi_answers_v2
WITH unrolled_answers as (
    SELECT * FROM `passculture-data-ehp.raw_stg.qpi_answers_v2` as qpi, qpi.answers as answers
),
question1 as (
    SELECT culturalsurvey_id as id,
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
    SELECT culturalsurvey_id as id,
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
    SELECT user_id, user_cultural_survey_id, user_civility, user_activity, FROM `passculture-data-ehp.clean_stg.applicative_database_user`
),
enriched_users as (
    SELECT user_id, booking_cnt as booking_count, first_connection_date FROM `passculture-data-ehp.analytics_stg.enriched_user_data`
)

select users.user_id, user_civility, users.user_activity,
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
