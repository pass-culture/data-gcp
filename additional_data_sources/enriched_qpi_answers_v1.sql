-- clean_stg.qpi_answers_v1
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
livres as (
    SELECT culturalsurvey_id as id,
    EXISTS(SELECT 1 FROM UNNEST(choices) choice WHERE choice IN ('Livre numérique', 'Livre audio')) as livre_numerique,
    ('Livre papier' in UNNEST(choices)) as livre_papier
    FROM unrolled_answers where question_id = "ga5bQ8RhPAEB"
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
bookings as (
    SELECT user_id, offer.offer_type,  venue_is_virtual FROM `passculture-data-ehp.analytics_stg.applicative_database_booking` booking
    LEFT JOIN `passculture-data-ehp.analytics_stg.applicative_database_stock` stock
    ON booking.stock_id = stock.stock_id
    LEFT JOIN `passculture-data-ehp.analytics_stg.applicative_database_offer` offer
    ON stock.offer_id = offer.offer_id
    LEFT JOIN `passculture-data-ehp.analytics_stg.applicative_database_venue` venue
    ON venue.venue_id = offer.venue_id
),
users AS (
    SELECT user_id, user_cultural_survey_id, user_civility, user_activity, FROM `passculture-data-ehp.clean_stg.applicative_database_user`
),
enriched_users as (
    SELECT user_id, booking_cnt as booking_count, first_connection_date FROM `passculture-data-ehp.analytics_stg.enriched_user_data`
)

select user_id,
IFNULL(culturalsurvey_id IN (select id from pratiques where film is true) and culturalsurvey_id in (select id from vod), false) as initial_audiovisuel,
IFNULL(culturalsurvey_id IN (select id from pratiques where film is true) and culturalsurvey_id in (select id from cinema), false) as initial_cinema,
IFNULL(culturalsurvey_id IN (select id from pratiques where jeux_video is true), false) as initial_jeux_videos,
IFNULL(culturalsurvey_id IN (select id from pratiques where lecture is true) and culturalsurvey_id in (select id from livres where livre_numerique is true), false) as initial_livre_numerique,
IFNULL(culturalsurvey_id IN (select id from pratiques where lecture is true) and culturalsurvey_id in (select id from livres where livre_papier  is true), false) as initial_livre_physique,
IFNULL(culturalsurvey_id IN (select id from pratiques where visite is true), false) as initial_musees_patrimoine,
IFNULL(
    culturalsurvey_id IN (select id from pratiques where festival is true)
    or (culturalsurvey_id IN (select id from pratiques where musique is true) and culturalsurvey_id in (select id from concerts)
), false) as initial_musique_live,
IFNULL(culturalsurvey_id IN (select id from pratiques where musique is true) and culturalsurvey_id in (select id from supports_musique where numerique is true), false) as initial_musique_numerique,
IFNULL(culturalsurvey_id IN (select id from pratiques where musique is true) and culturalsurvey_id in (select id from supports_musique where physique is true), false) as initial_musique_cd_vinyls,
IFNULL(culturalsurvey_id IN (select id from pratiques where pratique_artistique is true), false) as initial_pratique_artistique,
IFNULL(culturalsurvey_id IN (select id from pratiques where spectacle is true), false) as initial_spectacle_vivant,
IFNULL(culturalsurvey_id IN (select id from pratiques where pratique_artistique is true) and culturalsurvey_id IN (select id from pratique_musique), false) as initial_instrument,

IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'ThingType.AUDIOVISUEL'), False) as pass_audiovisuel,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.CINEMA', 'ThingType.CINEMA_ABO', 'ThingType.CINEMA_CARD')), False) as pass_cinema,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('ThingType.JEUX_VIDEO_ABO', 'ThingType.JEUX_VIDEO')), False) as pass_jeux_videos,
IFNULL(users.user_id IN (select user_id from bookings where (offer_type = 'ThingType.LIVRE_EDITION' and venue_is_virtual is true) or offer_type = 'ThingType.LIVRE_AUDIO'), False) as pass_livre_numerique,
IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'ThingType.LIVRE_EDITION' and venue_is_virtual is false), False) as pass_livre_physique,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.MUSEES_PATRIMOINE', 'ThingType.MUSEES_PATRIMOINE_ABO')), False) as pass_musees_patrimoine,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.MUSIQUE', 'ThingType.MUSIQUE_ABO')), False) as pass_musique_live,
IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'EventType.MUSIQUE' and venue_is_virtual is true), False) as pass_musique_numerique,
IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'EventType.MUSIQUE' and venue_is_virtual is false), False) as pass_musique_cd_vinyls,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.PRATIQUE_ARTISTIQUE', 'ThingType.PRATIQUE_ARTISTIQUE_ABO')), False) as pass_pratique_artistique,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.SPECTACLE_VIVANT', 'ThingType.SPECTACLE_VIVANT_ABO')), False) as pass_spectacle_vivant,
IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'ThingType.INSTRUMENT'), False) as pass_instrument,
IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'ThingType.PRESSE_ABO'), False) as pass_presse,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.CONFERENCE_DEBAT_DEDICACE', 'ThingType.OEUVRE_ART', 'EventType.JEUX')), False) as pass_autre
FROM `passculture-data-ehp.raw_stg.qpi_answers_v1` answers
INNER JOIN users
on answers.culturalsurvey_id = users.user_cultural_survey_id


-- enriched_qpi_answers_v2
WITH unrolled_answers as (
    SELECT * FROM `passculture-data-ehp.raw_stg.qpi_answers_v2` as qpi, qpi.answers as answers
),
question1 as (
    SELECT culturalsurvey_id as id,
    "pris un cours de pratique artistique (danse, théâtre, musique, dessin...) 🎨" IN UNNEST(choices) as initial_pratique_artistique,
    "participé à une conférence, une rencontre ou une découverte de métiers de la Culture 🎤" IN UNNEST(choices) as initial_autre,
    "allé à un musée, une visite ou une exposition  🏛" IN UNNEST(choices) as initial_musees_patrimoine,
    "assisté à une pièce de théâtre, à un spectacle de cirque, de danse... 💃" IN UNNEST(choices) as initial_spectacle_vivant,
    "allé à un concert ou un festival 🤘" IN UNNEST(choices) as initial_musique,
    "allé au cinéma 🎞" IN UNNEST(choices) as initial_cinema
    FROM  unrolled_answers
    WHERE question_id = "ge0Egr2m8V1T"
),
question2 as (
    SELECT culturalsurvey_id as id,
    "joué de ton instrument de musique 🎸" IN UNNEST(choices) as initial_instrument,
    "lu un article de presse 📰" IN UNNEST(choices) as initial_presse,
    "regardé un film chez toi 🍿" IN UNNEST(choices) as initial_audiovisuel,
    "joué à un jeu vidéo 🎮" IN UNNEST(choices) as initial_jeux_videos,
    "écouté de la musique ♫" IN UNNEST(choices) as initial_musique,
    "lu un livre 📚" IN UNNEST(choices) as initial_livre
    FROM  unrolled_answers
    WHERE question_id = "NeyLJOqShoHw"
),
bookings as (
    SELECT user_id, offer_type FROM `passculture-data-ehp.analytics_stg.applicative_database_booking` booking
    LEFT JOIN `passculture-data-ehp.analytics_stg.applicative_database_stock` stock
    ON booking.stock_id = stock.stock_id
    LEFT JOIN `passculture-data-ehp.analytics_stg.applicative_database_offer` offer
    ON stock.offer_id = offer.offer_id
),
users AS (
    SELECT user_id, user_cultural_survey_id, user_civility, user_activity, FROM `passculture-data-ehp.clean_stg.applicative_database_user`
),
enriched_users as (
    SELECT user_id, booking_cnt as booking_count, first_connection_date FROM `passculture-data-ehp.analytics_stg.enriched_user_data`
)

select users.user_id, user_civility, users.user_activity,
booking_count, TIMESTAMP_DIFF(CURRENT_TIMESTAMP(),  CAST(first_connection_date AS TIMESTAMP), DAY) as days_since_first_connection,
initial_pratique_artistique, initial_autre, initial_musees_patrimoine, initial_spectacle_vivant, initial_cinema, initial_instrument, initial_presse, initial_audiovisuel, initial_jeux_videos, initial_livre,
IFNULL(culturalsurvey_id IN (select id from question1 where initial_musique is true) or culturalsurvey_id in (select id from question2 where initial_musique is true), false) as initial_musique,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.PRATIQUE_ARTISTIQUE', 'ThingType.PRATIQUE_ARTISTIQUE_ABO')), False) as pass_pratique_artistique,
IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'EventType.CONFERENCE_DEBAT_DEDICACE'), False) as pass_autre,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.MUSEES_PATRIMOINE', 'ThingType.MUSEES_PATRIMOINE_ABO')), False) as pass_musees_patrimoine,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.SPECTACLE_VIVANT', 'ThingType.SPECTACLE_VIVANT_ABO')), False) as pass_spectacle_vivant,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.CINEMA', 'ThingType.CINEMA_CARD')), False) as pass_cinema,
IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'ThingType.INSTRUMENT'), False) as pass_instrument,
IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'ThingType.PRESSE_ABO'), False) as pass_presse,
IFNULL(users.user_id IN (select user_id from bookings where offer_type = 'ThingType.AUDIOVISUEL'), False) as pass_audiovisuel,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('ThingType.JEUX_VIDEO_ABO', 'ThingType.JEUX_VIDEO')), False) as pass_jeux_videos,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('ThingType.LIVRE_AUDIO', 'ThingType.LIVRE_EDITION')), False) as pass_livre,
IFNULL(users.user_id IN (select user_id from bookings where offer_type in ('EventType.MUSIQUE', 'ThingType.MUSIQUE', 'ThingType.MUSIQUE_ABO')), False) as pass_musique,
FROM (SELECT culturalsurvey_id FROM `passculture-data-ehp.raw_stg.qpi_answers_v2`) answers
INNER JOIN users
on answers.culturalsurvey_id = users.user_cultural_survey_id
INNER JOIN question1
on answers.culturalsurvey_id = question1.id
INNER JOIN question2
on answers.culturalsurvey_id = question2.id
INNER JOIN enriched_users
on users.user_id = enriched_users.user_id
