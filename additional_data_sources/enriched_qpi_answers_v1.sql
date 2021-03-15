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
)


select distinct(culturalsurvey_id) as id,
IFNULL(culturalsurvey_id IN (select id from pratiques where film is true) and culturalsurvey_id in (select id from vod), false) as audiovisuel,
IFNULL(culturalsurvey_id IN (select id from pratiques where film is true) and culturalsurvey_id in (select id from cinema), false) as cinema,
IFNULL(culturalsurvey_id IN (select id from pratiques where jeux_video is true), false) as jeux_videos,
IFNULL(culturalsurvey_id IN (select id from pratiques where lecture is true) and culturalsurvey_id in (select id from livres where livre_numerique is true), false) as livre_numerique,
IFNULL(culturalsurvey_id IN (select id from pratiques where lecture is true) and culturalsurvey_id in (select id from livres where livre_papier  is true), false) as livre_physique,
IFNULL(culturalsurvey_id IN (select id from pratiques where visite is true), false) as musees_patrimoine,
IFNULL(
    culturalsurvey_id IN (select id from pratiques where festival is true)
    or (culturalsurvey_id IN (select id from pratiques where musique is true) and culturalsurvey_id in (select id from concerts)
), false) as musique_live,
IFNULL(culturalsurvey_id IN (select id from pratiques where musique is true) and culturalsurvey_id in (select id from supports_musique where numerique is true), false) as musique_numerique,
IFNULL(culturalsurvey_id IN (select id from pratiques where musique is true) and culturalsurvey_id in (select id from supports_musique where physique is true), false) as musique_cd_vinyls,
IFNULL(culturalsurvey_id IN (select id from pratiques where pratique_artistique is true), false) as pratique_artistique,
IFNULL(culturalsurvey_id IN (select id from pratiques where spectacle is true), false) as spectacle_vivant,
IFNULL(culturalsurvey_id IN (select id from pratiques where pratique_artistique is true) and culturalsurvey_id IN (select id from pratique_musique), false) as instrument
FROM `passculture-data-ehp.raw_stg.qpi_answers_v1`
