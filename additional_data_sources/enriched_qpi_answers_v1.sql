-- enriched_qpi_answers_v1
-- In order to be able to conserve the answers of each user while a lot of user_id are missing in qpi_answers_v1,
-- we assign a new id for each row (and therefore each user) using ROW_NUMBER() OVER(). In addition, we still keep

WITH unrolled_answers as (
	SELECT * FROM (
		select *, ROW_NUMBER() OVER() as row_id from `passculture-data-ehp.clean_stg.qpi_answers_v1`
	) as qpi, qpi.answers as answers
)

select max(user_id) as user_id,
SUM(CAST("🎞 FILM & SÉRIE" IN UNNEST(choices) AS INT64)) > 0 and SUM(CAST(question_id = "Vo0aiAJsoymf" and choice != 'Jamais' AS INT64)) > 0 as cinema,
SUM(CAST("🎞 FILM & SÉRIE" IN UNNEST(choices) AS INT64)) > 0 and SUM(CAST(question_id = "dZmbwWSzroeN" and ARRAY_LENGTH(choices) > 0 AS INT64)) > 0 as audiovisuel,
SUM(CAST("🎮 JEU VIDÉO" IN UNNEST(choices) AS INT64)) > 0 as jeux_videos,
SUM(CAST("📚 LECTURE" IN UNNEST(choices) AS INT64)) > 0 as livre,
SUM(CAST("🏛 VISITE — musée, expo, monument..." IN UNNEST(choices) AS INT64)) > 0 as musees_patrimoine,
SUM(CAST("♫ MUSIQUE — écoute, concert" IN UNNEST(choices) AS INT64)) > 0 as musique,
SUM(CAST("🎸 PRATIQUE ARTISTIQUE — danse, instrument, écriture, dessin..." IN UNNEST(choices) AS INT64)) > 0 as pratique_artistique,
SUM(CAST("💃 SPECTACLE — théâtre, cirque, danse..." IN UNNEST(choices) AS INT64)) > 0 as spectacle_vivant,
SUM(CAST("🎸 PRATIQUE ARTISTIQUE — danse, instrument, écriture, dessin..." IN UNNEST(choices) AS INT64)) > 0 and SUM(CAST(question_id = "MxgbTe4j5Iee" and 'Faire de la musique ou du chant' in UNNEST(choices) AS INT64)) > 0  as instrument,
FROM unrolled_answers
group by row_id
