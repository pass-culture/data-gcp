select
    ident18 as respondent_id,
    e7 as listening_frequency_code,
    e81 as support_cd_cassette,
    e82 as support_vinyl,
    e83 as support_streaming_specialized,
    e84 as support_streaming_other,
    e85 as support_digital_files,
    e86 as support_radio,
    e87 as support_tv,
    e17 as music_at_home_frequency_code,
    e18 as active_listening_code,
    e19 as music_importance_code,
    case
        e7
        when 1
        then 'Oui, tous les jours ou presque'
        when 2
        then 'Oui, environ 3 ou 4 jours par semaine'
        when 3
        then 'Oui, environ 1 ou 2 jours par semaine'
        when 4
        then 'Oui, environ 1 à 3 jours par mois'
        when 5
        then 'Oui, plus rarement'
        when 6
        then 'Non, jamais ou pratiquement jamais'
        when 7
        then 'NSP'
        when 8
        then 'REF'
    end as listening_frequency,
    case
        e15 when 1 then 'Oui' when 2 then 'Non' when 3 then 'NSP' when 4 then 'REF'
    end as foreign_language_music,
    case
        e17
        when 1
        then 'Oui, tous les jours ou presque'
        when 2
        then 'Oui, de temps en temps'
        when 3
        then 'Oui, rarement'
        when 4
        then 'Non, jamais'
        when 5
        then 'NSP'
        when 6
        then 'REF'
    end as music_at_home_frequency,
    case
        e18
        when 1
        then 'Oui, tous les jours ou presque'
        when 2
        then 'Oui, de temps en temps'
        when 3
        then 'Oui, rarement'
        when 4
        then 'Non, jamais'
        when 5
        then 'NSP'
        when 6
        then 'REF'
    end as active_listening,
    case
        e19
        when 1
        then 'Oui, beaucoup'
        when 2
        then 'Oui, un peu'
        when 3
        then 'Non, pas tellement'
        when 4
        then 'Non, pas du tout'
        when 5
        then 'NSP'
        when 6
        then 'REF'
    end as music_importance
from {{ ref("int_seed__deps_cultural_practices_2018") }}
