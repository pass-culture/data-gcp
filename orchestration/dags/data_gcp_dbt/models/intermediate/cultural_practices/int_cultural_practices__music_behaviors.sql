-- Grain: 1 ligne = 1 répondant
-- Variables comportementales d'écoute musicale de l'enquête PC 2018.
-- Les préférences par genre (E10/E12/E13) sont gérées à part dans
-- int_cultural_practices__music_genres (format long respondent x genre).
select
    ident18 as respondent_id,
    -- E7 : fréquence d'écoute de musique (code catégoriel)
    e7 as listening_frequency,
    -- E8 : supports utilisés pour écouter (0/1 pour chaque support)
    e81 as support_cd_cassette,
    e82 as support_vinyl,
    e83 as support_streaming_specialized,
    e84 as support_streaming_other,
    e85 as support_digital_files,
    e86 as support_radio,
    e87 as support_tv,
    -- E15 : a écouté de la musique en langue étrangère (code catégoriel)
    e15 as foreign_language_music,
    -- E17 : met de la musique en rentrant chez soi (code catégoriel)
    e17 as music_at_home_frequency,
    -- E18 : écoute active, pour elle-même (code catégoriel)
    e18 as active_listening,
    -- E19 : importance émotionnelle de la musique (code catégoriel)
    e19 as music_importance
from `passculture-data-prod`.`seed_prod`.`deps_cultural_practices_2018`
